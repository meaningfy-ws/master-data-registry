import pandas as pd
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from pydantic import BaseModel
from starlette import status

from master_data_registry import config
from master_data_registry.services.organization_registry import get_organization_record_links

app = FastAPI()

DATAFRAME_JSON_KEY = "dataframe_json"
UNIQUE_COLUMN_NAME_KEY = "unique_column_name"


class OrganizationDeduplicationParams(BaseModel):
    dataframe_json: str
    unique_column_name: str


async def api_auth(creds: HTTPBasicCredentials = Depends(HTTPBasic())):
    username = creds.username
    password = creds.password
    if username == config.MASTER_DATA_REGISTRY_API_USER and password == config.MASTER_DATA_REGISTRY_API_PASSWORD:
        return "OK"

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Basic"},
    )


@app.post("/api/v1/dedup_organizations", dependencies=[Depends(api_auth)])
def dedup_organizations(params: OrganizationDeduplicationParams):
    dataframe_json = params.dataframe_json
    unique_column_name = params.unique_column_name
    dataframe = pd.read_json(dataframe_json)
    result_links = get_organization_record_links(organization_records=dataframe,
                                                 unique_column_name=unique_column_name)
    result_links.rename(columns={"unique_id_l": unique_column_name,
                                 "unique_id_r": f"OrganizationRegistryID"
                                 }, inplace=True)
    return result_links.to_json()
