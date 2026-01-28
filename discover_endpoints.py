"""
Discover and print all NBA API endpoint names.
"""

import inspect
import sys

try:
    import nba_api.stats.endpoints as endpoints_module
except ImportError as e:
    print(f"Error importing nba_api: {e}")
    print("Make sure nba-api is installed: pip install nba-api")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error importing nba_api: {e}")
    print(f"Error type: {type(e).__name__}")
    sys.exit(1)


def discover_all_endpoints():
    """Discover all endpoint classes from nba_api.stats.endpoints"""
    endpoint_names = []
    
    for name, obj in inspect.getmembers(endpoints_module):
        if inspect.isclass(obj) and name[0].isupper():
            endpoint_names.append(name)
    
    return sorted(endpoint_names)


def main():
    """Main execution"""
    print("🏀 NBA API Endpoints")
    print("=" * 60)
    
    endpoints = discover_all_endpoints()
    
    print(f"\nFound {len(endpoints)} endpoints:\n")
    
    for i, endpoint_name in enumerate(endpoints, 1):
        print(f"{i:3d}. {endpoint_name}")


if __name__ == "__main__":
    main()
