import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from google.cloud import bigquery

def main(request=None):
    # Meta credentials
    access_token = os.environ['FB_ACCESS_TOKEN']
    ad_account_id = os.environ['FB_AD_ACCOUNT_ID']
    app_id = os.environ['FB_APP_ID']
    app_secret = os.environ['FB_APP_SECRET']

    FacebookAdsApi.init(app_id, app_secret, access_token)
    account = AdAccount(ad_account_id)

    fields = ['campaign_name', 'impressions', 'clicks', 'spend', 'conversions', 'date_start']
    params = {
        'level': 'campaign',
        'time_increment': 1,
        'date_preset': 'yesterday'
    }

    insights = account.get_insights(fields=fields, params=params)
    rows = []

    for item in insights:
    rows.append({
        'date_start': item.get('date_start'),
        'campaign_name': item.get('campaign_name'),
        'adset_name': item.get('adset_name'),
        'ad_name': item.get('ad_name'),
        'impressions': int(item.get('impressions', 0)),
        'clicks': int(item.get('clicks', 0)),
        'spend': float(item.get('spend', 0)),
        'purchases': int(item.get('purchase', 0)),
        'conversion_value': float(item.get('conversion_value', 0)),
        'website_purchase_roas': float(item.get('website_purchase_roas', [{}])[0].get('value', 0))
    })


    bq = bigquery.Client()
    table_id = os.environ['BQ_TABLE_ID']
    errors = bq.insert_rows_json(table_id, rows)
    if errors:
        print(f"Errors: {errors}")
    else:
        print("Data inserted successfully.")
    return "Done"
