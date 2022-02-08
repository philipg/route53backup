import boto3
import os
import yaml
import time
from datetime import datetime
from botocore.exceptions import ClientError
import route53_utils

bucket_name = os.environ['BUCKET']
s3 = boto3.client('s3')
route53 = boto3.client('route53')
MAX_API_BATCH_SIZE = 500

def restore_hosted_zone(zone_to_restore):
    if zone_to_restore['Config']['PrivateZone']:
        restored_zone = route53.create_hosted_zone(
            Name=zone_to_restore['Name'],
            CallerReference=get_unique_caller_id(zone_to_restore['Id']),
            HostedZoneConfig=zone_to_restore['Config'],
            VPC=zone_to_restore['VPCs'][0]
        )['HostedZone']
    else:
        restored_zone = route53.create_hosted_zone(
            Name=zone_to_restore['Name'],
            CallerReference=get_unique_caller_id(zone_to_restore['Id']),
            HostedZoneConfig=zone_to_restore['Config']
        )['HostedZone']

    print('Restored the zone {}'.format(zone_to_restore['Id']))
    return restored_zone


def get_unique_caller_id(resource_id):
    """
    Creates a unique caller ID, which is required to avoid processing a single request multiple times by mistake
    :param resource_id: The ID of the resource to be restored
    :return: A unique string
    """
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", datetime.utcnow().utctimetuple())
    unique_caller_reference = '{}-{}'.format(timestamp, resource_id)
    return unique_caller_reference


def create_zone_if_not_exist(zone_obj):
    try:
        return route53.get_hosted_zone(Id=zone_obj['Id'])['HostedZone']
    except ClientError as e:
        if e.response['Error'].get('Code', False) and e.response['Error']['Code'] == 'NoSuchHostedZone':
            return restore_hosted_zone(zone_obj)
        else:
            print(e)



def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def main():
    zones = yaml.load(open('zones.yml', 'r'))
    for zone_obj in zones:
        zone = create_zone_if_not_exist(zone_obj)
        backup_zone_records = yaml.load(open(f"{zone_obj['Name']}yml", 'r'))
        current_zone_records = route53_utils.get_route53_zone_records(zone['Id'])

        records_to_upsert = list(filter(lambda x: x not in current_zone_records, backup_zone_records))

        ## TODO: add support for deletes
        changes_list = list(map(lambda x: {"Action": "UPSERT", "ResourceRecordSet": x}, records_to_upsert))

        if len(changes_list) > 0:
            print("making changes as length of changes higher than zero")
            print(changes_list)
            for batch in chunks(changes_list, MAX_API_BATCH_SIZE):
                route53.change_resource_record_sets(
                    HostedZoneId=zone['Id'],
                    ChangeBatch={'Changes': batch}
                )

    

    backup_health_checks = yaml.load(open('healthchecks.yml', 'r'))
    current_health_checks = route53_utils.get_route53_health_checks()

    # Compare the health checks by their IDs, actual objects are a little different
    if backup_health_checks:
        health_checks_to_create = list(filter(lambda x: x['Id'] not in list(map(lambda y: y['Id'], current_health_checks)), backup_health_checks))
        for health_check_to_create in health_checks_to_create:
            unique_caller_reference = get_unique_caller_id(health_check_to_create['Id'])
            created = route53.create_health_check(
                CallerReference=unique_caller_reference,
                HealthCheckConfig=health_check_to_create['HealthCheckConfig']
            )['HealthCheck']

            if len(health_check_to_create.get('Tags', [])) > 0:
                route53.change_tags_for_resource(ResourceType='healthcheck', ResourceId=created['Id'], AddTags=health_check_to_create['Tags'])

if __name__ == '__main__':
    main()