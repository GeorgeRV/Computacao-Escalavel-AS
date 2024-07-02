# pipeline_project/common/celery.py
from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery('pipeline_project',
             broker='pyamqp://guest@localhost//',
             backend='rpc://',
             include=['budget_simulator.budget_calculator',
                      'delivery_simulator.delivery_generator'])

app.conf.update(
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
