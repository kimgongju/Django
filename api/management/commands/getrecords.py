from django.core.management.base import BaseCommand, CommandError, CommandParser
from api.smarthome import get_records

class Command(BaseCommand):

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('entity', type=str)
        return super().add_arguments(parser)

    def handle(self, *args, **options):
        entity = options['entity']
        records = get_records(entity)
        print(f"{len(records)=}")