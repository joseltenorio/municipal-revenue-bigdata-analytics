"""Construccion Bronze de la Clasificacion Municipal MEF 2019.

Este script descarga los siete PDF oficiales del MEF, extrae sus tablas con
pdfplumber, genera CSV intermedios en Landing y consolida un unico dataset
Bronze Parquet para `municipal_classification`.

La fuente se integra por ubigeo, pero Bronze no realiza cruces ni joins con
SIAF, SISMEPRE o RENAMU.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber

from src.common.config import get_config_value, load_sources_config
from src.common.download import safe_download_file
from src.common.paths import PROJECT_ROOT, get_source_bronze_path, get_source_landing_path
from src.common.retry import build_retry_config, probe_with_fallback


SOURCE_NAME = "municipal_classification"
SOURCE_PAGE_URL_FIELD = "page_url"
SOURCE_YEAR = 2019
PDF_SUFFIX = ".pdf"
CSV_SUFFIX = ".csv"
TOTAL_EXPECTED_ROWS = 1874
EXPECTED_TYPES = {"A", "B", "C", "D", "E", "F", "G"}
EXPECTED_AMBITOS = {"provincial", "distrital"}
EXPECTED_COLUMNS = [
    "anio",
    "tipo_clasificacion",
    "ambito_municipal",
    "descripcion_tipo",
    "nro",
    "ubigeo",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_url",
    "bronze_source_page_url",
    "bronze_processed_at_utc",
]


class BronzeMunicipalClassificationBuildError(Exception):
    """Error controlado durante la construccion Bronze de la clasificacion."""


@dataclass(frozen=True)
class MunicipalClassificationResource:
    """Recurso PDF oficial del MEF seleccionado para ingesta."""

    resource_key: str
    file_name: str
    url: str
    tipo_clasificacion: str
    ambito_municipal: str
    descripcion_tipo: str
    expected_rows: int
    pdf_path: Path
    csv_path: Path


@dataclass(frozen=True)
class MunicipalClassificationPaths:
    """Rutas locales relevantes para la construccion Bronze."""

    landing_root: Path
    raw_dir: Path
    extracted_csv_dir: Path
    manifest_path: Path
    bronze_output_dir: Path


def utc_now_iso() -> str:
    """Retorna fecha y hora actual en UTC en formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def relative_to_project(path: Path) -> str:
    """Devuelve una ruta relativa al repositorio cuando sea posible."""

    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path.resolve()).replace("\\", "/")


def load_municipal_classification_config() -> dict[str, Any]:
    """Carga la configuracion de la fuente official del MEF."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise BronzeMunicipalClassificationBuildError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise BronzeMunicipalClassificationBuildError(
            f"La fuente '{SOURCE_NAME}' no esta habilitada."
        )

    return source_config


def build_source_paths(
    source_config: dict[str, Any],
    *,
    landing_dir: Path | None = None,
    bronze_dir: Path | None = None,
) -> MunicipalClassificationPaths:
    """Construye rutas locales para Landing y Bronze."""

    landing_subdir = str(source_config.get("landing_subdir") or SOURCE_NAME)
    bronze_subdir = str(source_config.get("bronze_subdir") or SOURCE_NAME)
    landing_root = landing_dir or get_source_landing_path(landing_subdir)
    bronze_output_dir = bronze_dir or get_source_bronze_path(bronze_subdir)

    return MunicipalClassificationPaths(
        landing_root=landing_root,
        raw_dir=landing_root / "raw",
        extracted_csv_dir=landing_root / "extracted_csv",
        manifest_path=landing_root / "manifest.json",
        bronze_output_dir=bronze_output_dir,
    )


def select_resources(
    source_config: dict[str, Any],
    paths: MunicipalClassificationPaths,
) -> list[MunicipalClassificationResource]:
    """Selecciona y valida los siete PDFs configurados."""

    candidate_resources = source_config.get("candidate_resources", {})
    if not isinstance(candidate_resources, dict) or not candidate_resources:
        raise BronzeMunicipalClassificationBuildError(
            "No existen candidate_resources para municipal_classification."
        )

    resources: list[MunicipalClassificationResource] = []

    for resource_key, resource in sorted(candidate_resources.items()):
        if not isinstance(resource, dict):
            continue
        if not resource.get("use_for_ingestion", False):
            continue

        file_name = str(resource.get("file_name") or "")
        url = str(resource.get("url") or "")
        tipo_clasificacion = str(resource.get("tipo_clasificacion") or "").upper()
        ambito_municipal = str(resource.get("ambito_municipal") or "").lower()
        descripcion_tipo = str(resource.get("descripcion_tipo") or "")
        expected_rows = int(resource.get("expected_rows") or 0)

        if not file_name or not url:
            raise BronzeMunicipalClassificationBuildError(
                f"El recurso '{resource_key}' requiere file_name y url."
            )
        if str(resource.get("format") or "").lower() != "pdf":
            raise BronzeMunicipalClassificationBuildError(
                f"El recurso '{resource_key}' debe usar format=pdf."
            )
        if tipo_clasificacion not in EXPECTED_TYPES:
            raise BronzeMunicipalClassificationBuildError(
                f"El recurso '{resource_key}' tiene tipo_clasificacion invalido: {tipo_clasificacion}."
            )
        if ambito_municipal not in EXPECTED_AMBITOS:
            raise BronzeMunicipalClassificationBuildError(
                f"El recurso '{resource_key}' tiene ambito_municipal invalido: {ambito_municipal}."
            )
        if expected_rows <= 0:
            raise BronzeMunicipalClassificationBuildError(
                f"El recurso '{resource_key}' debe definir expected_rows positivo."
            )

        pdf_stem = Path(file_name).stem
        resources.append(
            MunicipalClassificationResource(
                resource_key=resource_key,
                file_name=file_name,
                url=url,
                tipo_clasificacion=tipo_clasificacion,
                ambito_municipal=ambito_municipal,
                descripcion_tipo=descripcion_tipo,
                expected_rows=expected_rows,
                pdf_path=paths.raw_dir / file_name,
                csv_path=paths.extracted_csv_dir / f"{pdf_stem}{CSV_SUFFIX}",
            )
        )

    if len(resources) != 7:
        raise BronzeMunicipalClassificationBuildError(
            f"Se esperaban 7 recursos PDF habilitados y se encontraron {len(resources)}."
        )

    total_expected_rows = sum(resource.expected_rows for resource in resources)
    if total_expected_rows != TOTAL_EXPECTED_ROWS:
        raise BronzeMunicipalClassificationBuildError(
            "La suma de expected_rows no coincide con el total validado de la fuente. "
            f"Configurado={total_expected_rows}, validado={TOTAL_EXPECTED_ROWS}."
        )

    return resources


def validate_overwrite_policy(paths: MunicipalClassificationPaths, overwrite: bool) -> None:
    """Protege la salida Bronze frente a sobrescrituras accidentales."""

    if paths.bronze_output_dir.exists() and any(paths.bronze_output_dir.rglob("*.parquet")):
        if not overwrite:
            raise BronzeMunicipalClassificationBuildError(
                "Ya existe Bronze municipal_classification. Usa --overwrite para reemplazarlo."
            )


def ensure_directories(paths: MunicipalClassificationPaths) -> None:
    """Crea directorios de trabajo requeridos."""

    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.extracted_csv_dir.mkdir(parents=True, exist_ok=True)
    paths.bronze_output_dir.parent.mkdir(parents=True, exist_ok=True)


def probe_resource(url: str, timeout_seconds: int) -> dict[str, Any]:
    """Verifica disponibilidad remota con HEAD y fallback a GET."""

    retry_config = build_retry_config(
        source_name=SOURCE_NAME,
        timeout_seconds=timeout_seconds,
    )
    response = probe_with_fallback(url=url, retry_config=retry_config)
    try:
        content_length = response.headers.get("content-length")
        return {
            "http_status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "content_length_bytes": int(content_length)
            if content_length and content_length.isdigit()
            else None,
        }
    finally:
        response.close()


def download_pdf(
    *,
    resource: MunicipalClassificationResource,
    timeout_seconds: int,
    overwrite: bool,
) -> None:
    """Descarga un PDF oficial usando la utilidad comun de descargas."""

    retry_config = build_retry_config(
        source_name=SOURCE_NAME,
        timeout_seconds=timeout_seconds,
    )
    safe_download_file(
        url=resource.url,
        destination_path=resource.pdf_path,
        retry_config=retry_config,
        chunk_size=1024 * 1024,
        overwrite=overwrite,
        show_progress=False,
    )


def normalize_text(value: Any) -> str | None:
    """Normaliza texto extraido desde PDF conservando tildes."""

    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def flatten_table_row(raw_row: list[Any]) -> list[str]:
    """Colapsa una fila de pdfplumber eliminando celdas vacias intermedias."""

    return [value for value in (normalize_text(cell) for cell in raw_row) if value]


def is_header_row(values: list[str]) -> bool:
    """Identifica encabezados repetidos en cada pagina."""

    if not values:
        return False
    normalized = [value.upper() for value in values]
    return normalized[0] in {"N°", "Nº", "N"} and "UBIGEO" in normalized


def extract_rows_from_tables(pdf_bytes: bytes) -> list[list[str]]:
    """Extrae filas tabulares desde un PDF usando extract_tables()."""

    extracted_rows: list[list[str]] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                for raw_row in table:
                    values = flatten_table_row(raw_row)
                    if not values or is_header_row(values) or len(values) < 5:
                        continue

                    row = values[:5]
                    if not row[0].isdigit():
                        continue
                    if not re.fullmatch(r"^\d{6}$", row[1]):
                        continue

                    extracted_rows.append(row)

    return extracted_rows


def extract_rows_from_text(pdf_bytes: bytes) -> list[list[str]]:
    """Fallback basado en texto plano si extract_tables no devuelve filas."""

    extracted_rows: list[list[str]] = []
    row_pattern = re.compile(r"^(\d+)\s+(\d{6})\s+(.+?)\s{2,}(.+?)\s{2,}(.+)$")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for line in (page.extract_text() or "").splitlines():
                normalized_line = normalize_text(line)
                if not normalized_line or normalized_line.upper().startswith("N°"):
                    continue
                match = row_pattern.match(normalized_line)
                if not match:
                    continue
                extracted_rows.append(list(match.groups()))

    return extracted_rows


def extract_resource_dataframe(resource: MunicipalClassificationResource) -> pd.DataFrame:
    """Extrae y normaliza la tabla de un PDF del MEF."""

    if not resource.pdf_path.exists():
        raise BronzeMunicipalClassificationBuildError(
            f"No existe el PDF descargado: {resource.pdf_path}"
        )

    pdf_bytes = resource.pdf_path.read_bytes()
    rows = extract_rows_from_tables(pdf_bytes)
    if not rows:
        rows = extract_rows_from_text(pdf_bytes)

    if not rows:
        raise BronzeMunicipalClassificationBuildError(
            f"No se pudo extraer ninguna fila del PDF {resource.file_name}."
        )

    dataframe = pd.DataFrame(
        rows,
        columns=[
            "nro",
            "ubigeo",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
        ],
    )
    dataframe["anio"] = SOURCE_YEAR
    dataframe["tipo_clasificacion"] = resource.tipo_clasificacion
    dataframe["ambito_municipal"] = resource.ambito_municipal
    dataframe["descripcion_tipo"] = resource.descripcion_tipo
    dataframe["nro"] = pd.to_numeric(dataframe["nro"], errors="coerce").astype("Int64")
    dataframe["ubigeo"] = dataframe["ubigeo"].astype("string")

    for column in ["departamento_nombre", "provincia_nombre", "distrito_nombre"]:
        dataframe[column] = (
            dataframe[column]
            .astype("string")
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.upper()
        )

    dataframe = dataframe[
        [
            "anio",
            "tipo_clasificacion",
            "ambito_municipal",
            "descripcion_tipo",
            "nro",
            "ubigeo",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
        ]
    ]
    return dataframe


def write_extracted_csv(dataframe: pd.DataFrame, resource: MunicipalClassificationResource) -> None:
    """Escribe el CSV intermedio de una clasificacion."""

    resource.csv_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(resource.csv_path, index=False, encoding="utf-8")


def append_bronze_metadata(
    dataframe: pd.DataFrame,
    *,
    resource: MunicipalClassificationResource,
    source_page_url: str,
    processed_at_utc: str,
) -> pd.DataFrame:
    """Agrega metadata tecnica Bronze al subconjunto consolidado."""

    result = dataframe.copy()
    result["bronze_source_name"] = SOURCE_NAME
    result["bronze_resource_key"] = resource.resource_key
    result["bronze_source_file_name"] = resource.file_name
    result["bronze_source_file_path"] = relative_to_project(resource.pdf_path)
    result["bronze_source_url"] = resource.url
    result["bronze_source_page_url"] = source_page_url
    result["bronze_processed_at_utc"] = processed_at_utc
    return result[EXPECTED_COLUMNS]


def validate_resource_row_count(
    dataframe: pd.DataFrame,
    resource: MunicipalClassificationResource,
) -> None:
    """Valida conteo exacto por tipo."""

    actual_rows = int(len(dataframe))
    if actual_rows != resource.expected_rows:
        raise BronzeMunicipalClassificationBuildError(
            f"Conteo invalido para {resource.resource_key}: esperado={resource.expected_rows}, actual={actual_rows}."
        )


def validate_total_row_count(dataframe: pd.DataFrame) -> None:
    """Valida el total consolidado de filas."""

    actual_total = int(len(dataframe))
    if actual_total != TOTAL_EXPECTED_ROWS:
        raise BronzeMunicipalClassificationBuildError(
            f"Conteo total invalido: esperado={TOTAL_EXPECTED_ROWS}, actual={actual_total}."
        )


def validate_ubigeo_format(dataframe: pd.DataFrame) -> None:
    """Valida ubigeos de seis digitos sin perder ceros a la izquierda."""

    invalid = dataframe[
        ~dataframe["ubigeo"].astype("string").str.fullmatch(r"\d{6}", na=False)
    ]
    if not invalid.empty:
        sample = invalid["ubigeo"].astype("string").head(10).tolist()
        raise BronzeMunicipalClassificationBuildError(
            f"Se detectaron ubigeos invalidos. Muestra: {sample}"
        )


def validate_duplicates(dataframe: pd.DataFrame) -> None:
    """Valida duplicados por anio + ubigeo."""

    duplicates = dataframe[dataframe.duplicated(subset=["anio", "ubigeo"], keep=False)]
    if not duplicates.empty:
        sample = duplicates[["anio", "ubigeo"]].head(10).to_dict(orient="records")
        raise BronzeMunicipalClassificationBuildError(
            f"Se detectaron duplicados por anio + ubigeo. Muestra: {sample}"
        )


def validate_tipo_distribution(
    dataframe: pd.DataFrame,
    resources: list[MunicipalClassificationResource],
) -> dict[str, int]:
    """Valida distribucion exacta por tipo A-G."""

    actual_counts = dataframe.groupby("tipo_clasificacion").size().to_dict()
    expected_counts = {
        resource.tipo_clasificacion: resource.expected_rows for resource in resources
    }
    if actual_counts != expected_counts:
        raise BronzeMunicipalClassificationBuildError(
            f"Distribucion por tipo invalida. Esperado={expected_counts}, actual={actual_counts}."
        )
    return {str(key): int(value) for key, value in actual_counts.items()}


def write_bronze_dataset(
    dataframe: pd.DataFrame,
    bronze_output_dir: Path,
    *,
    overwrite: bool,
) -> None:
    """Escribe un unico dataset Parquet consolidado."""

    if bronze_output_dir.exists() and overwrite:
        shutil.rmtree(bronze_output_dir)
    elif bronze_output_dir.exists():
        raise BronzeMunicipalClassificationBuildError(
            f"Ya existe Bronze en {bronze_output_dir}. Usa --overwrite para reemplazar."
        )

    bronze_output_dir.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(bronze_output_dir / "data.parquet", index=False)


def build_manifest(
    *,
    paths: MunicipalClassificationPaths,
    source_page_url: str,
    processed_at_utc: str,
    resources: list[MunicipalClassificationResource],
    resource_results: list[dict[str, Any]],
    actual_total_rows: int,
) -> dict[str, Any]:
    """Construye el manifest final de la ejecucion exitosa."""

    return {
        "source_name": SOURCE_NAME,
        "source_page_url": source_page_url,
        "processed_at_utc": processed_at_utc,
        "expected_total_rows": TOTAL_EXPECTED_ROWS,
        "actual_total_rows": actual_total_rows,
        "resources": resource_results,
        "bronze_output_path": relative_to_project(paths.bronze_output_dir),
    }


def write_manifest(manifest: dict[str, Any], manifest_path: Path) -> None:
    """Escribe el manifest al final de una corrida exitosa."""

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_dry_run_summary(
    *,
    source_config: dict[str, Any],
    paths: MunicipalClassificationPaths,
    resources: list[MunicipalClassificationResource],
    timeout_seconds: int,
) -> dict[str, Any]:
    """Construye resumen de dry-run sin descargar ni escribir artefactos."""

    probe_results = []
    for resource in resources:
        probe_data = probe_resource(resource.url, timeout_seconds=timeout_seconds)
        probe_results.append(
            {
                "resource_key": resource.resource_key,
                "file_name": resource.file_name,
                "url": resource.url,
                "expected_rows": resource.expected_rows,
                "http_status_code": probe_data["http_status_code"],
                "content_type": probe_data["content_type"],
                "content_length_bytes": probe_data["content_length_bytes"],
                "pdf_path": relative_to_project(resource.pdf_path),
                "csv_path": relative_to_project(resource.csv_path),
            }
        )

    return {
        "source_name": SOURCE_NAME,
        "source_page_url": str(source_config.get(SOURCE_PAGE_URL_FIELD) or ""),
        "bronze_output_path": relative_to_project(paths.bronze_output_dir),
        "landing_root": relative_to_project(paths.landing_root),
        "expected_total_rows": TOTAL_EXPECTED_ROWS,
        "resources": probe_results,
        "downloads_performed": False,
        "parquet_written": False,
    }


def build_bronze_municipal_classification(
    *,
    source_config: dict[str, Any] | None = None,
    paths: MunicipalClassificationPaths | None = None,
    resources: list[MunicipalClassificationResource] | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    """Ejecuta la construccion Bronze de municipal_classification."""

    source_config = source_config or load_municipal_classification_config()
    paths = paths or build_source_paths(source_config)
    resources = resources or select_resources(source_config, paths)

    validate_overwrite_policy(paths, overwrite=overwrite)

    if dry_run:
        return build_dry_run_summary(
            source_config=source_config,
            paths=paths,
            resources=resources,
            timeout_seconds=timeout_seconds,
        )

    ensure_directories(paths)
    source_page_url = str(source_config.get(SOURCE_PAGE_URL_FIELD) or "")
    processed_at_utc = utc_now_iso()

    resource_results: list[dict[str, Any]] = []
    bronze_frames: list[pd.DataFrame] = []

    for resource in resources:
        download_pdf(
            resource=resource,
            timeout_seconds=timeout_seconds,
            overwrite=overwrite,
        )
        extracted = extract_resource_dataframe(resource)
        validate_resource_row_count(extracted, resource)
        write_extracted_csv(extracted, resource)
        bronze_frames.append(
            append_bronze_metadata(
                extracted,
                resource=resource,
                source_page_url=source_page_url,
                processed_at_utc=processed_at_utc,
            )
        )
        resource_results.append(
            {
                "resource_key": resource.resource_key,
                "tipo_clasificacion": resource.tipo_clasificacion,
                "url": resource.url,
                "pdf_path": relative_to_project(resource.pdf_path),
                "csv_path": relative_to_project(resource.csv_path),
                "expected_rows": resource.expected_rows,
                "actual_rows": int(len(extracted)),
                "status": "SUCCESS",
            }
        )

    bronze_dataframe = pd.concat(bronze_frames, ignore_index=True)
    validate_total_row_count(bronze_dataframe)
    validate_ubigeo_format(bronze_dataframe)
    validate_duplicates(bronze_dataframe)
    counts_by_tipo = validate_tipo_distribution(bronze_dataframe, resources)
    write_bronze_dataset(
        bronze_dataframe,
        paths.bronze_output_dir,
        overwrite=overwrite,
    )

    manifest = build_manifest(
        paths=paths,
        source_page_url=source_page_url,
        processed_at_utc=processed_at_utc,
        resources=resources,
        resource_results=resource_results,
        actual_total_rows=int(len(bronze_dataframe)),
    )
    write_manifest(manifest, paths.manifest_path)

    return {
        "source_name": SOURCE_NAME,
        "source_page_url": source_page_url,
        "bronze_output_path": relative_to_project(paths.bronze_output_dir),
        "landing_root": relative_to_project(paths.landing_root),
        "actual_total_rows": int(len(bronze_dataframe)),
        "counts_by_tipo": counts_by_tipo,
        "resources": resource_results,
        "manifest_path": relative_to_project(paths.manifest_path),
        "downloads_performed": True,
        "parquet_written": True,
    }


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Construye Bronze consolidado para municipal_classification."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida configuracion y URLs sin descargar ni escribir Parquet.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe Bronze y artefactos Landing si ya existen.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout en segundos para validacion y descarga de PDFs.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    summary = build_bronze_municipal_classification(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        timeout_seconds=args.timeout,
    )

    print("=" * 80)
    print("Bronze municipal_classification")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Landing root: {summary['landing_root']}")
    print(f"Bronze output: {summary['bronze_output_path']}")

    if args.dry_run:
        print(f"Total esperado: {summary['expected_total_rows']}")
        print("Descargas ejecutadas: no")
        for item in summary["resources"]:
            print(
                f"- {item['resource_key']} | filas_esperadas={item['expected_rows']} | "
                f"http={item['http_status_code']} | content_type={item['content_type']}"
            )
        print("Dry-run finalizado. No se descargaron PDFs ni se escribio Parquet.")
        return

    print(f"Total consolidado: {summary['actual_total_rows']}")
    print(f"Conteos por tipo: {summary['counts_by_tipo']}")
    print(f"Manifest: {summary['manifest_path']}")
    print("Construccion Bronze municipal_classification finalizada.")


if __name__ == "__main__":
    main()
