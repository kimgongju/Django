from django.core.management.base import BaseCommand
from api.hubspot import create_contact_properties, create_company_properties

class Command(BaseCommand):
    def handle(self, *args, **options):
        # create_contact_properties()
        create_company_properties()
