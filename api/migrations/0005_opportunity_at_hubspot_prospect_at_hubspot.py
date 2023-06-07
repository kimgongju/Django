# Generated by Django 4.0.1 on 2022-03-25 15:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0004_remove_opportunity_at_hubspot_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='opportunity',
            name='at_hubspot',
            field=models.TextField(blank=True, default='no', null=True),
        ),
        migrations.AddField(
            model_name='prospect',
            name='at_hubspot',
            field=models.TextField(blank=True, default='no', null=True),
        ),
    ]
