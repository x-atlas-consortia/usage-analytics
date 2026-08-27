#!/usr/bin/env python3

import argparse
import csv
import requests

BASE_URL = "https://hub.docker.com/v2/repositories"


def get_public_repositories(namespace):
    """Retrieve all public repositories for a Docker Hub namespace using pagination."""
    repos = []
    url = f"{BASE_URL}/{namespace}/?page_size=100"

    while url:
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()

            repos.extend(data.get("results", []))
            url = data.get("next")

        except requests.exceptions.RequestException as e:
            print(f"\n[Error] Failed to communicate with Docker Hub API: {e}")
            break

    return repos


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and aggregate pull counts for public Docker Hub repositories."
    )
    parser.add_argument("organization", help="Docker Hub organization or username")
    parser.add_argument("-o", "--output", help="Optional path to a CSV file to export the results")
    args = parser.parse_args()

    print(f"Fetching public repositories for '{args.organization}'...")
    repos = get_public_repositories(args.organization)

    # Filter out any repositories containing 'ubkg' in the name (case-insensitive)
    repos = [repo for repo in repos if "ubkg" not in repo.get("name", "").lower()]

    if not repos:
        print(f"No matching public repositories found for '{args.organization}'.")
        return

    # Sort repositories by pull count, highest to lowest
    repos.sort(key=lambda r: r.get("pull_count", 0), reverse=True)

    # Dynamically calculate clean column padding
    repo_width = max(max(len(repo["name"]) for repo in repos), 15) + 4
    date_width = 16
    pulls_width = 15
    total_width = repo_width + (date_width * 2) + pulls_width

    # Print Table Header
    print(f"\n{'Public Repository':<{repo_width}} {'Created Date':<{date_width}} {'Last Updated':<{date_width}} {'All-Time Pulls':>{pulls_width}}")
    print("-" * total_width)

    total_pulls = 0

    # Print Table Rows
    for repo in repos:
        pulls = repo.get("pull_count", 0)
        total_pulls += pulls
        
        # Clean up Created Date
        created_at = repo.get("date_registered", "N/A")
        if created_at != "N/A":
            created_at = created_at.split("T")[0]
            
        # Clean up Last Updated Date
        updated_at = repo.get("last_updated", "N/A")
        if updated_at != "N/A":
            updated_at = updated_at.split("T")[0]
            
        print(f"{repo['name']:<{repo_width}} {created_at:<{date_width}} {updated_at:<{date_width}} {pulls:>{pulls_width},}")

    # Print Summary Footer
    print("-" * total_width)
    footer_label = f"TOTAL (Count: {len(repos)})"
    print(f"{footer_label:<{repo_width + (date_width * 2)}} {total_pulls:>{pulls_width},}\n")

    # Export to CSV if requested
    if args.output:
        try:
            with open(args.output, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Repository Name", "Created Date", "Last Updated", "All-Time Pulls"])
                
                for repo in repos:
                    created_at = repo.get("date_registered", "N/A")
                    if created_at != "N/A":
                        created_at = created_at.split("T")[0]
                        
                    updated_at = repo.get("last_updated", "N/A")
                    if updated_at != "N/A":
                        updated_at = updated_at.split("T")[0]
                        
                    writer.writerow([repo["name"], created_at, updated_at, repo.get("pull_count", 0)])
                
                writer.writerow([])  # Blank spacer row
                writer.writerow([f"TOTAL (Count: {len(repos)})", "", "", total_pulls])
                
            print(f"[Success] Results successfully exported to: {args.output}")
        except IOError as e:
            print(f"[Error] Failed to write CSV file: {e}")


if __name__ == "__main__":
    main()