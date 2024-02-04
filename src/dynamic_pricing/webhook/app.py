from flask import Flask, request, jsonify
import time
import requests
import datetime
from requests.auth import HTTPBasicAuth
import os

# NOTE this code was built for tablet-based sites ONLY


def get_bearer_token(prod=False):
    if prod:
        url = "https://auth.developers.deliveroo.com/oauth2/token"
    else:
        url = "https://auth-sandbox.developers.deliveroo.com/oauth2/token"
    payload = {"grant_type": "client_credentials"}

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
    }

    if prod:
        client_id = os.getenv("PROD_CLIENT_ID")
        secret = os.getenv("PROD_SECRET")
    else:
        client_id = os.getenv("DEV_CLIENT_ID")
        secret = os.getenv("DEV_SECRET")
    response_access = requests.post(
        url, auth=HTTPBasicAuth(client_id, secret), data=payload, headers=headers
    )

    return response_access.json()["access_token"]


def sync_status(order_id: str, payload: dict, prod: bool = False):
    if prod:
        url = f"https://api.developers.deliveroo.com/order/v1/orders/{order_id}/sync_status"
    else:
        url = f"https://api-sandbox.developers.deliveroo.com/order/v1/orders/{order_id}/sync_status"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {get_bearer_token(prod)}",
    }

    print("sent payload ", payload)
    # NOTE make sure to check that the response sync is always given 200 response code
    response_sync = requests.post(url, json=payload, headers=headers)
    print(f"response_sync order {response_sync.text}")

    return response_sync.json()


# NOTE need to implement better most scenarios on the restaurant side

# only for tablet-less sites. There is a 'is_tabletless' field in the order object


def update_order_status(order_id: str, status: str, prod: bool = False):
    if prod:
        url = f"https://api.developers.deliveroo.com/order/v1/orders/{order_id}"
    else:
        url = f"https://api-sandbox.developers.deliveroo.com/order/v1/orders/{order_id}"

    status_payload = {
        "status": status,
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {get_bearer_token(prod)}",
    }

    response_order_update = requests.patch(url, json=status_payload, headers=headers)
    return response_order_update.json()


def webhook(prod=False):
    print("__________________________________________________________")

    data = request.get_json()
    if not prod:
        print(data)
    # current time like "2022-04-12T12:43:00.000Z"
    date_time = datetime.datetime.now().isoformat(timespec="milliseconds")[:-3] + "Z"
    payload = {"status": "succeeded", "occurred_at": date_time}

    # order rejected by restaurant
    if data["body"]["order"]["status"] == "rejected":
        return jsonify({"message": "Order rejected successfully"}), 200

    for item in data["body"]["order"]["items"]:
        if item["pos_item_id"] == "":
            payload = {
                "status": "failed",
                "reason": "pos_item_id_not_found",
                "notes": "id not found",
                "occurred_at": date_time,
            }
            break

    # NOTE: This is for items with incorrect pos_item_id (mismatched PLUS, according to Deliveroo)
    # payload = {
    #     "status": "failed",
    #     "reason": "pos_item_id_mismatched",
    #     "notes": "id not unique",
    #     "occurred_at": date_time,
    # }

    if data["event"] == "order.status_update":
        sync_status(data["body"]["order"]["id"], payload, prod)

    if data["body"]["order"]["status"] == "canceled":
        return jsonify({"message": "Order canceled successfully"}), 200

    return jsonify({"message": "Order received successfully"}), 200


app = Flask(__name__)

# make sure to encode the string to bytes
webhook_secret = b"your_webhook_secret"


@app.route("/dev-webhook", methods=["POST"])
def dev_webhook():
    return webhook(prod=False)


@app.route("/prod-webhook", methods=["POST"])
def prod_webhook():
    return webhook(prod=True)


@app.route("/", methods=["GET"])
def test():
    return jsonify({"message": "The API is working just load a valid URL"}), 200


if __name__ == "__main__":
    app.run(debug=False)
