import secrets
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from pydantic import BaseModel
from starlette import status

from master_data_registry import config
from master_data_registry.resources.duckdb_databases import ORGANIZATION_DUCKDB_DATABASE_PATH
from master_data_registry.services.organization_registry import get_organization_record_links

from master_data_registry.services.registry_management import remove_reference_table

app = FastAPI()

DATAFRAME_JSON_KEY = "dataframe_json"
UNIQUE_COLUMN_NAME_KEY = "unique_column_name"
DATAFRAME_TO_JSON_ORIENT_TYPE = "table"


class OrganizationDeduplicationParams(BaseModel):
    dataframe_json: str
    unique_column_name: str
    threshold_match_probability: Optional[float] = 0.8
    reference_table_name: Optional[str] = None
    linkage_model_config: Optional[dict] = None


def api_auth(credentials: HTTPBasicCredentials = Depends(HTTPBasic())):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = config.MASTER_DATA_REGISTRY_API_USER.encode("utf8")
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = config.MASTER_DATA_REGISTRY_API_PASSWORD.encode("utf8")
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect user or password!",
            headers={"WWW-Authenticate": "Basic"},
        )
    return "OK"


@app.post("/api/v1/dedup_and_link", dependencies=[Depends(api_auth)])
def dedup_organizations(params: OrganizationDeduplicationParams):
    try:
        dataframe_json = params.dataframe_json
        unique_column_name = params.unique_column_name
        threshold_match_probability = params.threshold_match_probability
        reference_table_name = params.reference_table_name
        linkage_model_config = params.linkage_model_config
        dataframe = pd.read_json(dataframe_json, orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    try:
        if unique_column_name not in dataframe.columns:
            raise ValueError(f"Unique column name {unique_column_name} is not in the dataframe")
        if not dataframe[unique_column_name].is_unique:
            raise ValueError(f"Unique column name {unique_column_name} is not unique in the dataframe")

        result_links = get_organization_record_links(organization_records=dataframe,
                                                     unique_column_name=unique_column_name,
                                                     threshold_match_probability=threshold_match_probability,
                                                     registry_duckdb_table_name=reference_table_name,
                                                     linkage_model_config=linkage_model_config
                                                     )
        result_links.rename(columns={"unique_id_l": "unique_id_src",
                                     "unique_id_r": "unique_id_dst",
                                     }, inplace=True)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    result_links_json = result_links.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    return result_links_json


@app.get("/api/v1/health")
def health():
    return {"status": "OK"}


class ReferenceTableParams(BaseModel):
    reference_table_name: str


@app.post("/api/v1/reference_tables/remove", dependencies=[Depends(api_auth)])
def remove_reference_table_from_registry(params: ReferenceTableParams):
    try:
        reference_table_name = params.reference_table_name
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    try:
        remove_result = remove_reference_table(reference_table_name=reference_table_name,
                                               duckdb_database_path=ORGANIZATION_DUCKDB_DATABASE_PATH)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return {"status": "OK", "remove_result": remove_result}
