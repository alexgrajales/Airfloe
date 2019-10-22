from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
import logging

def tell_slack_failed(context):
    #return SlackNequiOperator.tell_slack_failed(context)
    webhook = BaseHook.get_connection('Slack').password
    message = ':red_circle: AIRFLOW TASK FAILURE TIPS:\n' \
              'DAG:    {}\n' \
              'TASKS:  {}\n' \
              'Reason: {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id,
                context['exception'])
    alterHook = SlackWebhookOperator(
        task_id='integrate_slack',
        http_conn_id='Slack',
        webhook_token=webhook,
        message=message,
        username='airflow'
        )
    return alterHook.execute(context=context)


def tell_slack_success(context):
    webhook = BaseHook.get_connection('Slack').password
    message = ':white_check_mark: AIRFLOW TASK FINISH TIPS:\n' \
              'DAG:    {}\n' \
              'TASKS:  {}\n' \
        .format(context['task_instance'].dag_id,
                context['task_instance'].task_id)
    alterHook = SlackWebhookOperator(
        task_id='integrate_slack',
        http_conn_id='Slack',
        webhook_token=webhook,
        message=message,
        username='airflow'
        )
    return alterHook.execute(context=context)
