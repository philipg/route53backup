import os
import json
import time
from datetime import datetime
import boto3
import yaml
from botocore.exceptions import ClientError
import route53_utils

route53 = boto3.client("route53")

def get_route53_hosted_zones(next_dns_name=None, next_hosted_zone_id=None):
    if next_dns_name and next_hosted_zone_id:
        response = route53.list_hosted_zones_by_name(
            DNSName=next_dns_name, HostedZoneId=next_hosted_zone_id
        )
    else:
        response = route53.list_hosted_zones_by_name()
    zones = response["HostedZones"]
    if response["IsTruncated"]:
        zones += get_route53_hosted_zones(
            response["NextDNSName"], response["NextHostedZoneId"]
        )

    private_hosted_zones = list(filter(lambda x: x["Config"]["PrivateZone"], zones))
    for zone in private_hosted_zones:
        zone["VPCs"] = route53.get_hosted_zone(Id=zone["Id"])["VPCs"]
    return zones


def main():
    hosted_zones = get_route53_hosted_zones()

    with open(r"zones.yml", "w") as file:
        documents = yaml.dump(hosted_zones, file)

    for zone in hosted_zones:
        zone_records = route53_utils.get_route53_zone_records(zone["Id"])
        with open(f"{zone['Name']}yml", "w") as file:
            yaml.dump(zone_records, file)

    health_checks = route53_utils.get_route53_health_checks()
    for health_check in health_checks:
        tags = route53.list_tags_for_resource(
            ResourceType="healthcheck", ResourceId=health_check["Id"]
        )["ResourceTagSet"]["Tags"]
        health_check["Tags"] = tags
        with open("healthchecks.yml", "w") as file:
            yaml.dump(health_checks, file)


if __name__ == "__main__":
    main()
