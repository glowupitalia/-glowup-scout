import os

import requests


PRODUCT_FEES_URL = (
    "https://sellingpartnerapi-eu.amazon.com/products/fees/v0/feesEstimate"
)
MAX_BATCH_SIZE = 20


class ProductFeeBatchResults(list):
    """List-compatible batch payload carrying sanitized throttling metadata."""

    def __init__(self, values=(), *, retry_after=None, rate_limit=None):
        super().__init__(values)
        self.retry_after = retry_after
        self.rate_limit = rate_limit


def build_product_fee_requests(candidates, marketplace_id):
    if len(candidates) > MAX_BATCH_SIZE:
        raise ValueError("Product Fees batch cannot exceed 20 products")
    return [
        {
            "FeesEstimateRequest": {
                "MarketplaceId": marketplace_id,
                "IsAmazonFulfilled": True,
                "Identifier": candidate["identifier"],
                "PriceToEstimateFees": {
                    "ListingPrice": {
                        "Amount": candidate["price"],
                        "CurrencyCode": "EUR",
                    },
                },
            },
            "IdType": "ASIN",
            "IdValue": candidate["asin"],
        }
        for candidate in candidates
    ]


def search_product_fees_batch(
    candidates,
    token,
    *,
    marketplace_id=None,
    request_post=requests.post,
):
    if not candidates:
        return []
    marketplace_id = marketplace_id or os.environ["MARKETPLACE_ID"]
    body = build_product_fee_requests(candidates, marketplace_id)
    response = request_post(
        PRODUCT_FEES_URL,
        headers={
            "x-amz-access-token": token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        payload = data
    elif isinstance(data, dict) and isinstance(data.get("payload"), list):
        payload = data["payload"]
    else:
        raise ValueError("Unexpected Product Fees batch response")
    headers = getattr(response, "headers", {}) or {}
    return ProductFeeBatchResults(
        payload,
        retry_after=headers.get("Retry-After"),
        rate_limit=headers.get("x-amzn-RateLimit-Limit"),
    )
