import datetime
import os
import subprocess

from dateutil.relativedelta import relativedelta


class BackupCleaner:
    def __init__(self, dir_backup, options: dict = None):
        self.folder = dir_backup
        if not options:
            self.options = {"yearly": 5, "monthly": 12, "weekly": 5, "daily": 5}
        else:
            self.options.update(options)

    def list_yearly_backups(self):
        backups = []
        for i in range(self.options["yearly"] + 1):  # 0 to 5 years ago
            year_start = datetime.datetime.now().replace(
                month=1, day=1
            ) - relativedelta(years=i)
            year_end = year_start + relativedelta(years=1)
            backups.extend(
                sorted(self.get_files_in_date_range(year_start, year_end))[:1]
            )
        return backups

    def list_monthly_backups(self):
        backups = []
        for i in range(self.options["monthly"] + 1):
            month_start = datetime.datetime.now().replace(day=1) - relativedelta(months=i)
            next_month = month_start + relativedelta(months=1)
            print(f"Checking for backups between {month_start} and {next_month}")  # Debugging
            backups.extend(
                sorted(self.get_files_in_date_range(month_start, next_month))[:1]
            )
        return backups

    def list_weekly_backups(self):
        backups = []
        for i in range(self.options["weekly"] + 1):
            today = datetime.datetime.now()
            monday = today - datetime.timedelta(days=today.weekday()) - datetime.timedelta(weeks=i)
            next_monday = monday + datetime.timedelta(days=7)
            print(f"Looking for Monday backups from {monday} to {next_monday}")  # Debugging
            backups.extend(sorted(self.get_files_in_date_range(monday, next_monday))[:1])
        return backups

    def list_daily_backups(self):
        backups = []
        for i in range(self.options["daily"] + 1):  # Last 7 days
            day_start = datetime.datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - datetime.timedelta(days=i)
            day_end = day_start + datetime.timedelta(days=1)
            backups.extend(self.get_files_in_date_range(day_start, day_end))
        return backups

    def get_files_in_date_range(self, start_date, end_date):
        files = []
        for file in os.listdir(self.folder):
            file_path = os.path.join(self.folder, file)
            if os.path.isfile(file_path):
                mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                if start_date <= mod_time < end_date:
                    files.append(file)
        return files

    def get_all_backups(self):
        yearly = self.list_yearly_backups()
        monthly = self.list_monthly_backups()
        weekly = self.list_weekly_backups()
        daily = self.list_daily_backups()
        return set(yearly + monthly + weekly + daily)

    def list_backups_to_delete(self):
        all_backups = self.get_all_backups()
        return [f for f in os.listdir(self.folder) if f not in all_backups]

    def clean_old_backups(self):
        backups_to_delete = self.list_backups_to_delete()
        for file_to_delete in backups_to_delete:
            file_path = os.path.join(self.folder, file_to_delete)
            subprocess.run(["rm", "-rf", file_path])
