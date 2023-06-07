from email.policy import default
import string
from django.core.management.base import BaseCommand, CommandError, CommandParser
# from api.views import save_contacts
# from api.views import save_accounts
from api.views import export_xlsx

class Command(BaseCommand):
    '''Command'''
    def add_arguments(self, parser):
        '''Create command getting sample followed by a list'''
        parser.add_argument(
            '-m',
            '--model',
            nargs='+',
            type=str,
            help='pull custom model'
        )
    def handle(self, *args, **options):
        models = options['model'] if options['model'] else None
        export_xlsx(models)