import requests
import json


class MenderAPI():
    def __init__(self, endpoint, username, password):
        self._endpoint = endpoint

        self._login(username, password)

    def _login(self, username, password):
        session = requests.Session()
        session.auth = (username, password)

        r = session.post(f"{self._endpoint}/api/management/v1/useradm/auth/login",
                         headers={
                             'Content-Type': 'application/json',
                             'Accept': 'application/jwt',
                             'Authorization': 'Basic {access-token}'
                         })
        if r.ok:
            jwt_token = r.text

            self._session = requests.Session()
            self._session.headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': f'Bearer {jwt_token}'
            }
        else:
            raise Exception(f"Failed to login: '{r.json()['error']}'")

    def _api_request(self, method, url, data=None):
        return getattr(self._session, method.lower())(f'{self._endpoint}/api/{url}', data=data)

    def get_device_by_attribute(self, attr, value):
        data = {
            "filters": [
                {
                    "attribute": attr,
                    "scope": "identity",
                    "type": "$eq",
                    "value": value
                }
            ]
        }

        r = self._api_request("POST", "management/v2/inventory/filters/search", data=json.dumps(data))
        if r.ok:
            return r.json()
        else:
            raise Exception(f"Failed to get device `{attr}`: '{r.json()['error']}'")

    def get_devices(self, devices_per_page=100, complete=False):
        devices = {}

        page = 1
        while True:
            print(f"Reading devices page {page}")
            r = self._api_request("GET",
                                  f"management/v1/inventory/devices?per_page={devices_per_page}&page={page}")

            if r.ok:
                device_page = r.json()

                for d in device_page:
                    device_info = {}
                    for attr in d['attributes']:
                        attributes = ["quattId",
                                      "QUATT_BUILD_VERSION", "device_type"]
                        if complete is True or attr["name"] in attributes:
                            device_info[attr["name"]] = attr["value"]
                    if "QUATT_BUILD_VERSION" in device_info:
                        device_info['id'] = d['id']
                        device_info["QUATT_BUILD_VERSION"] = device_info["QUATT_BUILD_VERSION"].strip(
                            '"')
                        devices[device_info["quattId"]] = device_info

                if len(device_page) < devices_per_page:
                    break
                page = page + 1
            else:
                raise Exception(
                    f"Failed to get devices: '{r.json()['error']}'")

        return devices

    def get_releases(self):
        r = self._api_request("GET", "management/v1/deployments/deployments/releases/list")
        if r.ok:
            return r.json()
        else:
            raise Exception(f"Failed to get releases: '{r.json()['error']}'")

    def start_deployment(self, release, devices):
        print(f"Deploying release '{release}' to {len(devices)} devices")

        body = {
            "name": release,
            "artifact_name": release,
            "devices": devices,
            "retries": 3
        }

        r = self._api_request("POST", f"management/v1/deployments/deployments", data=json.dumps(body))

        if not r.ok:
            raise Exception(
                f"Failed to start deployment for {release}: '{r.json()['error']}'")

    def get_device_auth_sets(self, device):
        r = self._api_request("GET", f"management/v2/devauth/devices/{device}")
        if r.ok:
            return r.json()['auth_sets']
        else:
            raise Exception(f"Failed to get authentication sets for {device}: '{r.json()['error']}'")

    def delete_auth_set(self, device, auth_id):
        r = self._api_request("DELETE", f"management/v2/devauth/devices/{device}/{auth_id}")
        if r.ok:
            print(f"Deleted authentication set {auth_id} for {device}")
        else:
            raise Exception(f"Failed to delete authentication sets for {device}: '{r.json()['error']}'")

    def decommission(self, device):
        r = self._api_request("DELETE", f"management/v2/devauth/devices/{device}")
        if r.ok:
            print(f"Decommissioned {device}")
        else:
            raise Exception(f"Failed to decommission {device}: '{r.json()['error']}'")

    def pre_authorize(self, identity, pubkey):
        data = {
            "identity_data": identity,
            "pubkey": pubkey
        }

        r = self._api_request("POST", 'management/v2/devauth/devices', data=json.dumps(data))
        if r.ok:
            print(f"Pre-authorized {identity['hostName']}")
        else:
            raise Exception(f"Failed to pre-authorize {identity['hostName']}: '{r.json()['error']}'")
