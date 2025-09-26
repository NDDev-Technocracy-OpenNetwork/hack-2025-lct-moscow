import aiohttp
import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from app.config import settings

logger = logging.getLogger(__name__)


class AirflowClient:
    """Client for interacting with Airflow REST API"""

    def __init__(self):
        self.base_url = settings.AIRFLOW_BASE_URL
        self.username = settings.AIRFLOW_USERNAME
        self.password = settings.AIRFLOW_PASSWORD
        self.auth = aiohttp.BasicAuth(self.username, self.password)

    async def trigger_dag(self, job_id: str, dag_config: Dict[str, Any]) -> str:
        """Trigger a DAG run with configuration"""

        dag_id = "bigdata_processing_dag"
        dag_run_id = f"run_{job_id}_{int(datetime.utcnow().timestamp())}"

        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"

        payload = {
            "dag_run_id": dag_run_id,
            "conf": dag_config
        }

        async with aiohttp.ClientSession(auth=self.auth) as session:
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"DAG triggered successfully: {dag_run_id}")
                        return dag_run_id
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to trigger DAG: {response.status} - {error_text}")
                        raise Exception(f"Airflow API error: {response.status}")

            except Exception as e:
                logger.error(f"Error triggering DAG: {e}")
                raise

    async def get_dag_run_status(self, dag_run_id: str) -> Dict[str, Any]:
        """Get DAG run status"""

        dag_id = "bigdata_processing_dag"
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"

        async with aiohttp.ClientSession(auth=self.auth) as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Failed to get DAG status: {response.status}")
                        return {}
            except Exception as e:
                logger.error(f"Error getting DAG status: {e}")
                return {}

    async def cancel_dag_run(self, dag_run_id: str) -> bool:
        """Cancel a running DAG"""

        dag_id = "bigdata_processing_dag"
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"

        payload = {"state": "failed"}

        async with aiohttp.ClientSession(auth=self.auth) as session:
            try:
                async with session.patch(url, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"DAG run cancelled: {dag_run_id}")
                        return True
                    else:
                        logger.error(f"Failed to cancel DAG: {response.status}")
                        return False
            except Exception as e:
                logger.error(f"Error cancelling DAG: {e}")
                return False