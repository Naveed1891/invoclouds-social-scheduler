#!/usr/bin/env python3
"""Queue one due social post in Buffer for Instagram and Facebook.

Expected posts.csv columns:
publish_date,image_filename,instagram_caption,facebook_caption,
instagram_post_id,facebook_post_id

Required environment variable:
    BUFFER_API_KEY

PUBLIC_BASE_URL is optional in GitHub Actions because it can be derived from
GITHUB_REPOSITORY. For local runs, set it to your GitHub Pages site, for example:
    https://YOUR-USERNAME.github.io/invoclouds-social-scheduler

Optional environment variables (only needed if automatic discovery is
ambiguous): BUFFER_ORGANIZATION_ID, BUFFER_INSTAGRAM_CHANNEL_ID,
BUFFER_FACEBOOK_CHANNEL_ID.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


BUFFER_API_URL = "https://api.buffer.com"
CSV_PATH = Path(os.getenv("POSTS_CSV", "posts.csv"))
REQUIRED_COLUMNS = (
    "publish_date",
    "image_filename",
    "instagram_caption",
    "facebook_caption",
    "instagram_post_id",
    "facebook_post_id",
)


class PublishError(RuntimeError):
    """A clear, actionable publishing error."""


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise PublishError(f"Missing required environment variable: {name}")
    return value


def public_base_url() -> str:
    configured = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if configured:
        return configured

    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    if "/" in repository:
        owner, repo = repository.split("/", 1)
        return f"https://{owner}.github.io/{repo}"

    raise PublishError(
        "Set PUBLIC_BASE_URL to your GitHub Pages address, for example "
        "https://YOUR-USERNAME.github.io/invoclouds-social-scheduler"
    )


def graphql_request(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Call Buffer GraphQL and handle both HTTP and GraphQL errors."""
    token = required_env("BUFFER_API_KEY")
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    request = Request(
        BUFFER_API_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "invoclouds-social-scheduler/1.0",
        },
    )

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with urlopen(request, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))

            if result.get("errors"):
                first = result["errors"][0]
                code = first.get("extensions", {}).get("code", "UNKNOWN")
                message = first.get("message", "Unknown Buffer GraphQL error")
                if code == "RATE_LIMIT_EXCEEDED" and attempt < 3:
                    time.sleep(2**attempt)
                    continue
                raise PublishError(f"Buffer API error ({code}): {message}")

            data = result.get("data")
            if data is None:
                raise PublishError(f"Buffer returned no data: {result}")
            return data

        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = PublishError(f"Buffer HTTP error {exc.code}: {body}")
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise last_error from exc
        except (URLError, TimeoutError) as exc:
            last_error = PublishError(f"Could not reach Buffer: {exc}")
            if attempt == 3:
                raise last_error from exc

        time.sleep(2**attempt)

    raise PublishError(str(last_error or "Buffer request failed"))


def get_organization_id() -> str:
    configured = os.getenv("BUFFER_ORGANIZATION_ID", "").strip()
    if configured:
        return configured

    data = graphql_request(
        """
        query GetOrganizations {
          account {
            organizations { id name }
          }
        }
        """
    )
    organizations = data.get("account", {}).get("organizations", [])
    if not organizations:
        raise PublishError("No Buffer organization was found for this API key.")
    if len(organizations) > 1:
        choices = ", ".join(f"{item['name']} ({item['id']})" for item in organizations)
        raise PublishError(
            "More than one Buffer organization was found. Add the repository "
            f"variable BUFFER_ORGANIZATION_ID. Available organizations: {choices}"
        )
    return organizations[0]["id"]


def get_channels(organization_id: str) -> list[dict[str, Any]]:
    data = graphql_request(
        """
        query GetChannels($organizationId: OrganizationId!) {
          channels(input: { organizationId: $organizationId }) {
            id
            name
            displayName
            service
            isQueuePaused
          }
        }
        """,
        {"organizationId": organization_id},
    )
    return data.get("channels", [])


def select_channel(channels: list[dict[str, Any]], service: str) -> dict[str, Any]:
    override_name = f"BUFFER_{service.upper()}_CHANNEL_ID"
    override_id = os.getenv(override_name, "").strip()
    if override_id:
        matching_id = [channel for channel in channels if channel.get("id") == override_id]
        if not matching_id:
            raise PublishError(
                f"{override_name} does not match a channel in the selected organization."
            )
        return matching_id[0]

    matches = [
        channel
        for channel in channels
        if str(channel.get("service", "")).strip().lower() == service
    ]
    if not matches:
        available = ", ".join(
            f"{channel.get('displayName') or channel.get('name')} "
            f"[{channel.get('service')}]"
            for channel in channels
        )
        raise PublishError(
            f"No {service} channel was found in Buffer. Available channels: {available}"
        )
    if len(matches) > 1:
        choices = ", ".join(
            f"{channel.get('displayName') or channel.get('name')} ({channel['id']})"
            for channel in matches
        )
        raise PublishError(
            f"More than one {service} channel was found. Add repository variable "
            f"{override_name}. Available channels: {choices}"
        )
    if matches[0].get("isQueuePaused"):
        raise PublishError(
            f"The Buffer queue for {service} is paused. Unpause it before running this workflow."
        )
    return matches[0]


def read_posts() -> tuple[list[str], list[dict[str, str]]]:
    if not CSV_PATH.exists():
        raise PublishError(f"CSV file not found: {CSV_PATH}")

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
        if missing:
            raise PublishError(
                "posts.csv is missing these columns: " + ", ".join(missing)
            )
        rows = list(reader)

    if not rows:
        raise PublishError("posts.csv has no post rows.")
    return fieldnames, rows


def write_posts(fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    temporary = CSV_PATH.with_suffix(CSV_PATH.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, CSV_PATH)


def choose_due_row(rows: list[dict[str, str]]) -> tuple[int, dict[str, str]] | None:
    today = date.today()
    candidates: list[tuple[date, int, dict[str, str]]] = []

    for index, row in enumerate(rows, start=2):
        raw_date = row.get("publish_date", "").strip()
        try:
            scheduled_date = date.fromisoformat(raw_date)
        except ValueError as exc:
            raise PublishError(
                f"Invalid publish_date on CSV line {index}: {raw_date!r}. Use YYYY-MM-DD."
            ) from exc

        needs_instagram = not row.get("instagram_post_id", "").strip()
        needs_facebook = not row.get("facebook_post_id", "").strip()
        if scheduled_date <= today and (needs_instagram or needs_facebook):
            candidates.append((scheduled_date, index - 2, row))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    _, row_index, row = candidates[0]
    return row_index, row


def image_url_for(filename: str) -> str:
    clean_name = filename.strip()
    if not clean_name or Path(clean_name).name != clean_name:
        raise PublishError(
            f"Invalid image_filename {filename!r}; enter only a filename, not a path."
        )

    local_image = Path("images") / clean_name
    if not local_image.is_file():
        raise PublishError(f"Image is missing from the repository: {local_image}")

    return f"{public_base_url()}/images/{quote(clean_name)}"


def verify_public_image(image_url: str) -> None:
    request = Request(
        image_url,
        method="GET",
        headers={"User-Agent": "invoclouds-social-scheduler/1.0"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            content_type = response.headers.get_content_type()
            response.read(64)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise PublishError(
            f"The image is not publicly reachable yet: {image_url}. "
            "Check GitHub Pages and try again."
        ) from exc

    if not content_type.startswith("image/"):
        raise PublishError(
            f"The public URL returned {content_type}, not an image: {image_url}"
        )


def create_buffer_post(
    channel_id: str,
    service: str,
    caption: str,
    image_url: str,
) -> str:
    post_input = {
        "text": caption,
        "channelId": channel_id,
        "schedulingType": "automatic",
        "mode": "addToQueue",
        "assets": [{"image": {"url": image_url}}],
    }

    # Buffer requires an explicit post type for Instagram and Facebook.
    # Instagram also requires shouldShareToFeed for a normal feed post.
    if service == "instagram":
        post_input["metadata"] = {
            "instagram": {
                "type": "post",
                "shouldShareToFeed": True,
            }
        }
    elif service == "facebook":
        post_input["metadata"] = {"facebook": {"type": "post"}}
    else:
        raise PublishError(f"Unsupported Buffer service: {service}")
    data = graphql_request(
        """
        mutation CreatePost($input: CreatePostInput!) {
          createPost(input: $input) {
            ... on PostActionSuccess {
              post { id dueAt status }
            }
            ... on MutationError {
              message
            }
          }
        }
        """,
        {"input": post_input},
    )

    result = data.get("createPost") or {}
    post = result.get("post")
    if not post:
        raise PublishError(
            "Buffer rejected the post: " + result.get("message", "Unknown mutation error")
        )

    print(
        f"Buffer post created: id={post['id']}, "
        f"status={post.get('status')}, dueAt={post.get('dueAt')}"
    )
    return post["id"]


def main() -> int:
    fieldnames, rows = read_posts()
    selected = choose_due_row(rows)
    if selected is None:
        print(f"No unpublished post is due on or before {date.today().isoformat()}.")
        return 0

    row_index, row = selected
    filename = row["image_filename"].strip()
    image_url = image_url_for(filename)
    print(f"Preparing {filename} from CSV line {row_index + 2}.")
    verify_public_image(image_url)
    print(f"Public image verified: {image_url}")

    organization_id = get_organization_id()
    channels = get_channels(organization_id)
    instagram = select_channel(channels, "instagram")
    facebook = select_channel(channels, "facebook")

    # Save posts.csv after each successful channel. The workflow will commit
    # this progress even if the second channel fails, preventing duplicates.
    if not row["instagram_post_id"].strip():
        caption = row["instagram_caption"].strip()
        if not caption:
            raise PublishError("The Instagram caption is empty for the selected row.")
        print(f"Adding to Instagram queue: {instagram.get('displayName') or instagram.get('name')}")
        row["instagram_post_id"] = create_buffer_post(
            instagram["id"], "instagram", caption, image_url
        )
        write_posts(fieldnames, rows)
    else:
        print("Instagram is already recorded for this row; skipping it.")

    if not row["facebook_post_id"].strip():
        caption = row["facebook_caption"].strip()
        if not caption:
            raise PublishError("The Facebook caption is empty for the selected row.")
        print(f"Adding to Facebook queue: {facebook.get('displayName') or facebook.get('name')}")
        row["facebook_post_id"] = create_buffer_post(
            facebook["id"], "facebook", caption, image_url
        )
        write_posts(fieldnames, rows)
    else:
        print("Facebook is already recorded for this row; skipping it.")

    print("Done. Both channel post IDs are recorded in posts.csv.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PublishError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
