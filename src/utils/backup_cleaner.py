import datetime
import os
import subprocess

from dateutil.relativedelta import relativedelta


class BackupCleaner:
    """Manages and cleans up backup files.

    This class provides functionality to list, filter, and delete backup files based on
    a specified retention policy.
    """
    def __init__(self, dir_backup: str, options: dict = None):
        """Initializes BackupCleaner with backup directory and retention options.

        Args:
            dir_backup (str): The directory containing the backup files.
            options (dict, optional): A dictionary specifying the retention policy.
                Defaults to keeping 5 yearly, 12 monthly, 5 weekly, and 5 daily backups.
        """
        self.folder = dir_backup
        if not options:
            self.options = {"yearly": 5, "monthly": 12, "weekly": 5, "daily": 5}
        else:
            self.options.update(options)

    def list_yearly_backups(self) -> list:
        """Lists yearly backups to keep.

        Returns:
            list: A list of yearly backup filenames to keep.
        """
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

    def list_monthly_backups(self) -> list:
        """Lists monthly backups to keep.

        Returns:
            list: A list of monthly backup filenames to keep.
        """
        backups = []
        for i in range(self.options["monthly"] + 1):
            month_start = datetime.datetime.now().replace(day=1) - relativedelta(months=i)
            next_month = month_start + relativedelta(months=1)
            print(f"Checking for backups between {month_start} and {next_month}")  # Debugging
            backups.extend(
                sorted(self.get_files_in_date_range(month_start, next_month))[:1]
            )
        return backups

    def list_weekly_backups(self) -> list:
        """Lists weekly backups to keep.

        Returns:
            list: A list of weekly backup filenames to keep.
        """
        backups = []
        for i in range(self.options["weekly"] + 1):
            today = datetime.datetime.now()
            monday = today - datetime.timedelta(days=today.weekday()) - datetime.timedelta(weeks=i)
            next_monday = monday + datetime.timedelta(days=7)
            print(f"Looking for Monday backups from {monday} to {next_monday}")  # Debugging
            backups.extend(sorted(self.get_files_in_date_range(monday, next_monday))[:1])
        return backups

    def list_daily_backups(self) -> list:
        """Lists daily backups to keep.

        Returns:
            list: A list of daily backup filenames to keep.
        """
        backups = []
        for i in range(self.options["daily"] + 1):  # Last 7 days
            day_start = datetime.datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - datetime.timedelta(days=i)
            day_end = day_start + datetime.timedelta(days=1)
            backups.extend(self.get_files_in_date_range(day_start, day_end))
        return backups

    def get_files_in_date_range(self, start_date: datetime.datetime, end_date: datetime.datetime) -> list:
        """Retrieves files within a specific date range.

        This method filters files in the backup directory based on their modification time,
        returning only those files modified within the specified start and end dates.

        Args:
            start_date (datetime.datetime): The start of the date range.
            end_date (datetime.datetime): The end of the date range.

        Returns:
            list: A list of filenames modified within the date range.
        """
        files = []
        for file in os.listdir(self.folder):
            file_path = os.path.join(self.folder, file)
            if os.path.isfile(file_path):
                mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                if start_date <= mod_time < end_date:
                    files.append(file)
        return files

    def get_all_backups(self) -> set:
        """Retrieves all backups to keep according to the retention policy.

        This method combines the lists of yearly, monthly, weekly, and daily backups to keep,
        returning a set of unique filenames.

        Returns:
            set: A set of backup filenames to keep.
        """
        yearly = self.list_yearly_backups()
        monthly = self.list_monthly_backups()
        weekly = self.list_weekly_backups()
        daily = self.list_daily_backups()
        return set(yearly + monthly + weekly + daily)

    def list_backups_to_delete(self) -> list:
        """Lists backups to delete according to the retention policy.

        This method identifies backup files in the backup directory that are not part of the
        set of backups to keep, determined by the retention policy.

        Returns:
            list: A list of backup filenames to delete.
        """
        all_backups = self.get_all_backups()
        return [f for f in os.listdir(self.folder) if f not in all_backups]

    def clean_old_backups(self) -> None:
        """Cleans up old backup files.

        This method deletes backup files identified by `list_backups_to_delete`,
        effectively enforcing the retention policy.
        """
        backups_to_delete = self.list_backups_to_delete()
        for file_to_delete in backups_to_delete:
            file_path = os.path.join(self.folder, file_to_delete)
            subprocess.run(["rm", "-rf", file_path])
