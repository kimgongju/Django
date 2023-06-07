from django.core.management.base import BaseCommand
from api.hubspot import get_pipeline_id

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        get_pipeline_id()