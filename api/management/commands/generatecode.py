from django.core.management.base import BaseCommand, CommandError, CommandParser
# from api.views import save_contacts
from api.views import save_accounts
from api.views import save_salesforce_contacts
from api.views import save_opportunities
from api.views import save_prospect, save_prospect_2, generate_prospect
from api.views import save_opportunities_pardot, save_advisorschoice_policy

class Command(BaseCommand):

    def handle(self, *args, **options):
        # save_contacts(None)
        # save_accounts(None)
        # save_salesforce_contacts(None)
        # save_opportunities(None)
        # save_prospect(None)
        # generate_prospect(None)
        save_prospect_2(None)
        # save_opportunities_pardot(None)
        # save_advisorschoice_policy(None)