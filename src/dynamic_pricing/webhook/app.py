"""
This module defines webhook endpoints for handling order updates and status synchronizations
with a third-party API.
"""

import datetime
import os
import requests
from flask import Flask, jsonify, request
from requests.auth import HTTPBasicAuth
from dynamic_pricing.core.order_manager import insert_order_data
from dynamic_pricing.core.db_utils import get_db_connection
from dynamic_pricing.webhook.config import (
    BASE_URL_DEV,
    BASE_URL_PROD,
    AUTH_URL_DEV,
    AUTH_URL_PROD,
)

app = Flask(__name__)


def get_api_url(order_id, prod):
    """Get the API URL for the delivery service based on the environment."""
    base_url = BASE_URL_PROD if prod else BASE_URL_DEV
    return f"{base_url}/{order_id}"


def get_auth_url(prod):
    """Gets the authentication URL for the delivery service
    based on the environment."""
    return AUTH_URL_PROD if prod else AUTH_URL_DEV


def get_bearer_token(prod=False):
    """Retrieve the bearer token for authentication from the delivery service API."""
    url = get_auth_url(prod)
    payload = {"grant_type": "client_credentials"}
    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
    }
    client_id = os.getenv("PROD_CLIENT_ID" if prod else "DEV_CLIENT_ID")
    secret = os.getenv("PROD_SECRET" if prod else "DEV_SECRET")

    response = requests.post(
        url,
        auth=HTTPBasicAuth(client_id, secret),
        data=payload,
        headers=headers,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def sync_status(order_id, payload, prod=False):
    """Synchronize the status of an order with the delivery service API."""
    url = get_api_url(order_id, prod) + "/sync_status"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {get_bearer_token(prod)}",
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_order_status(order_id, status, prod=False):
    """Update the status of an order in the delivery service database."""
    url = get_api_url(order_id, prod)
    payload = {"status": status}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {get_bearer_token(prod)}",
    }
    response = requests.patch(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


@app.route("/dev-webhook", methods=["POST"])
def dev_webhook():
    """Webhook endpoint for development environment."""
    return handle_webhook(False)


@app.route("/prod-webhook", methods=["POST"])
def prod_webhook():
    """Webhook endpoint for production environment."""
    return handle_webhook(True)


@app.route("/", methods=["GET"])
def test():
    """API test endpoint."""
    return (
        jsonify({"message": "The API is working just load a valid URL"}),
        200,
    )


def handle_webhook(prod):
    """Handle incoming webhook requests to update order statuses or receive notifications."""
    data = request.get_json()
    if not prod:
        app.logger.info(
            data
        )  # Using logging instead of print for better practice

    occurred_at = (
        datetime.datetime.now().isoformat(timespec="milliseconds") + "Z"
    )
    payload = {"status": "succeeded", "occurred_at": occurred_at}

    if data["body"]["order"]["status"] == "rejected":
        return jsonify({"message": "Order rejected successfully"}), 200

    for item in data["body"]["order"]["items"]:
        if not item["pos_item_id"]:
            return (
                jsonify(
                    {
                        "status": "failed",
                        "reason": "pos_item_id_not_found",
                        "notes": "id not found",
                        "occurred_at": occurred_at,
                    }
                ),
                400,
            )

    if data["event"] == "order.status_update":
        sync_status(data["body"]["order"]["id"], payload, prod)

    if data["body"]["order"]["status"] == "canceled":
        return jsonify({"message": "Order canceled successfully"}), 200

    connection = get_db_connection()
    insert_order_data(
        connection,
        data["body"]["order"]["restaurant"]["name"],
        data["body"]["order"],
        is_webhook=True,
    )
    return jsonify({"message": "Order received successfully"}), 200


if __name__ == "__main__":
    app.run(debug=False)
