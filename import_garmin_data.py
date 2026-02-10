#!/usr/bin/env python3
"""
Garmin Data Import Script
Extracts health and fitness data from Garmin Connect for analysis/archival

This script directly uses the Garmin API client (without MCP) for bulk data extraction.
"""

import os
import json
import datetime
from pathlib import Path
from dotenv import load_dotenv
import argparse
from typing import Dict, List, Any, Optional
from dateutil.parser import parse
from dateutil.rrule import rrule, DAILY

# Load environment variables
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

from garth.exc import GarthHTTPError
from garminconnect import Garmin, GarminConnectAuthenticationError


class GarminDataImporter:
    """Handles bulk data import from Garmin Connect"""
    
    def __init__(self, email: str, password: str):
        """Initialize the importer with credentials"""
        self.email = email
        self.password = password
        self.garmin = None
        self.tokenstore = os.path.expanduser(os.getenv("GARMINTOKENS") or "~/.garminconnect")
        
    def connect(self) -> bool:
        """Establish connection to Garmin Connect"""
        try:
            print(f"Attempting to connect using token data from '{self.tokenstore}'...")
            self.garmin = Garmin()
            self.garmin.login(self.tokenstore)
            print("Connected successfully using stored tokens.")
            return True
            
        except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError):
            print("Stored tokens not found or expired. Logging in with credentials...")
            try:
                self.garmin = Garmin(email=self.email, password=self.password, is_cn=False)
                # garminconnect prefers tokenstore if env var GARMINTOKENS is set.
                # For the *first* login (no token files yet), force credential login by unsetting it.
                os.environ.pop("GARMINTOKENS", None)
                self.garmin.login(tokenstore=None)
                # Save tokens for future use
                Path(self.tokenstore).mkdir(parents=True, exist_ok=True)
                self.garmin.garth.dump(self.tokenstore)
                print(f"Login successful. Tokens saved to '{self.tokenstore}'")
                return True
            except Exception as e:
                print(f"Failed to connect: {str(e)}")
                return False
    
    def get_date_range_data(self, start_date: datetime.date, end_date: datetime.date, 
                           data_types: List[str]) -> Dict[str, Any]:
        """Extract data for a date range"""
        results = {
            'metadata': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'extraction_date': datetime.datetime.now().isoformat(),
                'data_types': data_types
            },
            'data': {}
        }
        
        # Iterate through each day in the range
        for dt in rrule(DAILY, dtstart=start_date, until=end_date):
            date_str = dt.strftime('%Y-%m-%d')
            print(f"\nProcessing {date_str}...")
            
            daily_data = {}
            
            # Extract requested data types
            for data_type in data_types:
                try:
                    if data_type == 'stats':
                        daily_data['stats'] = self.garmin.get_stats(date_str)
                    
                    elif data_type == 'steps':
                        daily_data['steps'] = self.garmin.get_steps_data(date_str)
                    
                    elif data_type == 'heart_rate':
                        daily_data['heart_rate'] = self.garmin.get_heart_rates(date_str)
                    
                    elif data_type == 'sleep':
                        daily_data['sleep'] = self.garmin.get_sleep_data(date_str)
                    
                    elif data_type == 'body_composition':
                        daily_data['body_composition'] = self.garmin.get_body_composition(date_str)
                    
                    elif data_type == 'hydration':
                        daily_data['hydration'] = self.garmin.get_hydration_data(date_str)
                    
                    elif data_type == 'respiration':
                        daily_data['respiration'] = self.garmin.get_respiration_data(date_str)
                    
                    elif data_type == 'spo2':
                        daily_data['spo2'] = self.garmin.get_spo2_data(date_str)
                    
                    elif data_type == 'stress':
                        daily_data['stress'] = self.garmin.get_stress_data(date_str)
                    
                    elif data_type == 'user_summary':
                        daily_data['user_summary'] = self.garmin.get_user_summary(date_str)
                    
                    elif data_type == 'personal_record':
                        daily_data['personal_record'] = self.garmin.get_personal_record(date_str)
                    
                    elif data_type == 'rhr':
                        daily_data['rhr'] = self.garmin.get_rhr_day(date_str)
                    
                    print(f"  ✓ {data_type}")
                    
                except Exception as e:
                    print(f"  ✗ {data_type}: {str(e)}")
                    daily_data[data_type] = {'error': str(e)}
            
            results['data'][date_str] = daily_data
        
        return results
    
    def get_activities(self, limit: int = 100, start: int = 0) -> List[Dict[str, Any]]:
        """Get activities with pagination"""
        try:
            activities = self.garmin.get_activities(start, limit)
            print(f"Retrieved {len(activities)} activities")
            return activities
        except Exception as e:
            print(f"Error retrieving activities: {str(e)}")
            return []
    
    def get_activity_details(self, activity_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific activity"""
        try:
            # Get basic activity data
            activity = self.garmin.get_activity_evaluation(activity_id)
            
            # Get additional details
            details = {
                'basic_info': activity,
                'splits': None,
                'split_summaries': None,
                'weather': None,
                'hr_zones': None,
                'gear': None
            }
            
            try:
                details['splits'] = self.garmin.get_activity_splits(activity_id)
            except: pass
            
            try:
                details['split_summaries'] = self.garmin.get_activity_split_summaries(activity_id)
            except: pass
            
            try:
                details['weather'] = self.garmin.get_activity_weather(activity_id)
            except: pass
            
            try:
                details['hr_zones'] = self.garmin.get_activity_hr_in_timezones(activity_id)
            except: pass
            
            try:
                details['gear'] = self.garmin.get_activity_gear(activity_id)
            except: pass
            
            return details
        except Exception as e:
            print(f"Error retrieving activity details: {str(e)}")
            return {}
    
    def export_to_file(self, data: Any, filename: str, format: str = 'json'):
        """Export data to file"""
        output_dir = Path('garmin_exports')
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        if format == 'json':
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            print(f"Data exported to {filepath}")
        else:
            print(f"Unsupported format: {format}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Import data from Garmin Connect')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='Number of days to import (default: 7)')
    parser.add_argument('--data-types', nargs='+', 
                       default=['stats', 'steps', 'heart_rate', 'sleep'],
                       help='Data types to import')
    parser.add_argument('--activities', action='store_true', help='Import activities')
    parser.add_argument('--activity-limit', type=int, default=10, help='Number of activities to import')
    parser.add_argument('--activity-details', action='store_true', help='Include detailed activity data')
    
    args = parser.parse_args()
    
    # Get credentials
    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    
    if not email or not password:
        print("ERROR: GARMIN_EMAIL and GARMIN_PASSWORD must be set in environment (e.g. via .env or systemd EnvironmentFile)")
        return
    
    # Initialize importer
    importer = GarminDataImporter(email, password)
    
    if not importer.connect():
        print("Failed to connect to Garmin Connect")
        return
    
    # Determine date range
    if args.end_date:
        end_date = parse(args.end_date).date()
    else:
        end_date = datetime.date.today()
    
    if args.start_date:
        start_date = parse(args.start_date).date()
    else:
        start_date = end_date - datetime.timedelta(days=args.days - 1)
    
    # Import daily data
    print(f"\nImporting data from {start_date} to {end_date}")
    print(f"Data types: {', '.join(args.data_types)}")
    
    daily_data = importer.get_date_range_data(start_date, end_date, args.data_types)
    
    # Export daily data
    filename = f"garmin_daily_{start_date}_{end_date}.json"
    importer.export_to_file(daily_data, filename)
    
    # Import activities if requested
    if args.activities:
        print(f"\nImporting {args.activity_limit} most recent activities...")
        activities = importer.get_activities(limit=args.activity_limit)
        
        activity_data = {
            'metadata': {
                'extraction_date': datetime.datetime.now().isoformat(),
                'activity_count': len(activities)
            },
            'activities': []
        }
        
        for idx, activity in enumerate(activities):
            print(f"Processing activity {idx+1}/{len(activities)}: {activity.get('activityName', 'Unknown')}")
            
            activity_info = {
                'summary': activity,
                'details': None
            }
            
            if args.activity_details and 'activityId' in activity:
                activity_info['details'] = importer.get_activity_details(str(activity['activityId']))
            
            activity_data['activities'].append(activity_info)
        
        # Export activity data
        filename = f"garmin_activities_{datetime.date.today()}.json"
        importer.export_to_file(activity_data, filename)
    
    print("\nImport complete!")


if __name__ == "__main__":
    main()
