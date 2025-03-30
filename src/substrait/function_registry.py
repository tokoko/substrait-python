from substrait.gen.proto.type_pb2 import Type
from importlib.resources import files as importlib_files
import itertools
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional, Union
from .derivation_expression import evaluate, _evaluate, _parse

import yaml


DEFAULT_URI_PREFIX = "https://github.com/substrait-io/substrait/blob/main/extensions"


# mapping from argument types to shortened signature names: https://substrait.io/extensions/#function-signature-compound-names
_normalized_key_names = {
    "i8": "i8",
    "i16": "i16",
    "i32": "i32",
    "i64": "i64",
    "fp32": "fp32",
    "fp64": "fp64",
    "string": "str",
    "binary": "vbin",
    "boolean": "bool",
    "timestamp": "ts",
    "timestamp_tz": "tstz",
    "date": "date",
    "time": "time",
    "interval_year": "iyear",
    "interval_day": "iday",
    "interval_compound": "icompound",
    "uuid": "uuid",
    "fixedchar": "fchar",
    "varchar": "vchar",
    "fixedbinary": "fbin",
    "decimal": "dec",
    "precision_time": "pt",
    "precision_timestamp": "pts",
    "precision_timestamp_tz": "ptstz",
    "struct": "struct",
    "list": "list",
    "map": "map",
}


def normalize_substrait_type_names(typ: str) -> str:
    # Strip type specifiers
    typ = typ.split("<")[0]
    # First strip nullability marker
    typ = typ.strip("?").lower()

    if typ.startswith("any"):
        return "any"
    elif typ.startswith("u!"):
        return typ
    elif typ in _normalized_key_names:
        return _normalized_key_names[typ]
    else:
        raise Exception(f"Unrecognized substrait type {typ}")


def violates_integer_option(actual: int, option, parameters: dict):
    if isinstance(option, SubstraitTypeParser.NumericLiteralContext):
        return actual != int(str(option.Number()))
    elif isinstance(option, SubstraitTypeParser.NumericParameterNameContext):
        parameter_name = str(option.Identifier())
        if parameter_name in parameters and parameters[parameter_name] != actual:
            return True
        else:
            parameters[parameter_name] = actual
    else:
        raise Exception(
            f"Input should be either NumericLiteralContext or NumericParameterNameContext, got {type(option)} instead"
        )

    return False


from substrait.gen.antlr.SubstraitTypeParser import SubstraitTypeParser


def types_equal(type1: Type, type2: Type, check_nullability=False):
    if check_nullability:
        return type1 == type2
    else:
        x, y = Type(), Type()
        x.CopyFrom(type1)
        y.CopyFrom(type2)
        x.__getattribute__(
            x.WhichOneof("kind")
        ).nullability = Type.Nullability.NULLABILITY_UNSPECIFIED
        y.__getattribute__(
            y.WhichOneof("kind")
        ).nullability = Type.Nullability.NULLABILITY_UNSPECIFIED
        return x == y


def covers(
    covered: Type,
    covering: SubstraitTypeParser.TypeLiteralContext,
    parameters: dict,
    check_nullability=False,
):
    if isinstance(covering, SubstraitTypeParser.TypeParamContext):
        parameter_name = str(covering.Identifier())

        if parameter_name in parameters:
            covering = parameters[parameter_name]

            return types_equal(covering, covered, check_nullability)
        else:
            parameters[parameter_name] = covered
            return True

    covering = covering.type_()
    scalar_type = covering.scalarType()
    if scalar_type:
        covering = _evaluate(covering, {})
        return types_equal(covering, covered, check_nullability)

    parameterized_type = covering.parameterizedType()
    if parameterized_type:
        if isinstance(parameterized_type, SubstraitTypeParser.DecimalContext):
            if covered.WhichOneof("kind") != "decimal":
                return False

            nullability = (
                Type.NULLABILITY_NULLABLE
                if parameterized_type.isnull
                else Type.NULLABILITY_REQUIRED
            )

            if (
                check_nullability
                and nullability
                != covered.__getattribute__(covered.WhichOneof("kind")).nullability
            ):
                return False

            return not (
                violates_integer_option(
                    covered.decimal.scale, parameterized_type.scale, parameters
                )
                or violates_integer_option(
                    covered.decimal.precision, parameterized_type.precision, parameters
                )
            )
        else:
            raise Exception(f"Unhandled type {type(parameterized_type)}")

    any_type = covering.anyType()
    if any_type:
        return True


class FunctionEntry:
    def __init__(
        self, uri: str, name: str, impl: Mapping[str, Any], anchor: int
    ) -> None:
        self.name = name
        self.normalized_inputs: list = []
        self.uri: str = uri
        self.anchor = anchor
        self.arguments = []
        self.rtn = impl["return"]
        self.nullability = impl.get("nullability", "MIRROR")
        self.variadic = impl.get("variadic", False)
        if input_args := impl.get("args", []):
            for val in input_args:
                if typ := val.get("value"):
                    self.arguments.append(_parse(typ))
                    self.normalized_inputs.append(normalize_substrait_type_names(typ))
                elif arg_name := val.get("name", None):
                    self.arguments.append(val.get("options"))
                    self.normalized_inputs.append("req")

    def __repr__(self) -> str:
        return f"{self.name}:{'_'.join(self.normalized_inputs)}"

    def satisfies_signature(self, signature: tuple) -> Optional[str]:
        if self.variadic:
            min_args_allowed = self.variadic.get("min", 0)
            if len(signature) < min_args_allowed:
                return None
            inputs = [self.arguments[0]] * len(signature)
        else:
            inputs = self.arguments
        if len(inputs) != len(signature):
            return None

        zipped_args = list(zip(inputs, signature))

        parameters = {}

        for x, y in zipped_args:
            if type(y) == str:
                if y not in x:
                    return None
            else:
                if not covers(
                    y, x, parameters, check_nullability=self.nullability == "DISCRETE"
                ):
                    return None

        output_type = evaluate(self.rtn, parameters)

        if self.nullability == "MIRROR":
            sig_contains_nullable = any(
                [
                    p.__getattribute__(p.WhichOneof("kind")).nullability
                    == Type.NULLABILITY_NULLABLE
                    for p in signature
                    if type(p) == Type
                ]
            )
            output_type.__getattribute__(output_type.WhichOneof("kind")).nullability = (
                Type.NULLABILITY_NULLABLE
                if sig_contains_nullable
                else Type.NULLABILITY_REQUIRED
            )

        return output_type


class FunctionRegistry:
    def __init__(self, load_default_extensions=True) -> None:
        self._uri_mapping: dict = defaultdict(dict)
        self._uri_id_generator = itertools.count(1)

        self._function_mapping: dict = defaultdict(dict)
        self._id_generator = itertools.count(1)

        self._uri_aliases = {}

        if load_default_extensions:
            for fpath in importlib_files("substrait.extensions").glob(  # type: ignore
                "functions*.yaml"
            ):
                uri = f"{DEFAULT_URI_PREFIX}/{fpath.name}"
                self._uri_aliases[fpath.name] = uri
                self.register_extension_yaml(fpath, uri)

    def register_extension_yaml(
        self,
        fname: Union[str, Path],
        uri: str,
    ) -> None:
        fname = Path(fname)
        with open(fname) as f:  # type: ignore
            extension_definitions = yaml.safe_load(f)

        self.register_extension_dict(extension_definitions, uri)

    def register_extension_dict(self, definitions: dict, uri: str) -> None:
        self._uri_mapping[uri] = next(self._uri_id_generator)
        
        for named_functions in definitions.values():
            for function in named_functions:
                for impl in function.get("impls", []):
                    func = FunctionEntry(
                        uri, function["name"], impl, next(self._id_generator)
                    )
                    if (
                        func.uri in self._function_mapping
                        and function["name"] in self._function_mapping[func.uri]
                    ):
                        self._function_mapping[func.uri][function["name"]].append(func)
                    else:
                        self._function_mapping[func.uri][function["name"]] = [func]

    # TODO add an optional return type check
    def lookup_function(
        self, uri: str, function_name: str, signature: tuple
    ) -> Optional[tuple[FunctionEntry, Type]]:
        uri = self._uri_aliases.get(uri, uri)

        if (
            uri not in self._function_mapping
            or function_name not in self._function_mapping[uri]
        ):
            return None
        functions = self._function_mapping[uri][function_name]
        for f in functions:
            assert isinstance(f, FunctionEntry)
            rtn = f.satisfies_signature(signature)
            if rtn is not None:
                return (f, rtn)

        return None
    
    def lookup_uri(self, uri: str) -> Optional[int]:
        uri = self._uri_aliases.get(uri, uri)
        return self._uri_mapping.get(uri, None)
