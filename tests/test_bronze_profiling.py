"""Pruebas unitarias mínimas para utilidades de profiling Bronze."""

from src.quality.profile_bronze_datasets import (
    candidate_keys_for_resource,
    is_null_like_literal,
    normalize_for_matching,
)


def test_null_like_literals() -> None:
    assert is_null_like_literal(" ")
    assert is_null_like_literal("NULL")
    assert is_null_like_literal("S/N")
    assert not is_null_like_literal("0")


def test_normalize_for_matching_municipal_names() -> None:
    assert normalize_for_matching("M. D. DE CHETO") == "CHETO"
    assert normalize_for_matching("Municipalidad Distrital de Huáncas") == "HUANCAS"


def test_candidate_keys_for_known_sources() -> None:
    siaf_keys = candidate_keys_for_resource("siaf_income", "daily_2026")
    assert ["sec_ejec"] in siaf_keys
    assert ["ano_doc", "mes_doc", "sec_ejec"] in siaf_keys

    sismepre_keys = candidate_keys_for_resource("sismepre", "esat_estadistica_atm")
    assert ["ano_aplicacion", "periodo", "sec_ejec", "formulario_id", "ano_estadistica", "mes_estadistica"] in sismepre_keys

    renamu_keys = candidate_keys_for_resource("renamu", "base_renamu_2022")
    assert ["ubigeo"] in renamu_keys
