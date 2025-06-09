import schedule
import time
import subprocess

def job():
    print("Running ETL pipeline...")
    subprocess.run(["python", "pipeline.py"])
    print("ETL run completed.")

def main():
    print("Choose scheduling option:")
    print("1. Run daily at specific time (HH:MM, 24-hour format)")
    print("2. Run every N hours")

    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        time_str = input("Enter time to run daily (HH:MM): ").strip()
        # Validate time format HH:MM
        try:
            hour, minute = map(int, time_str.split(":"))
            assert 0 <= hour < 24 and 0 <= minute < 60
        except (ValueError, AssertionError):
            print("Invalid time format. Please enter in HH:MM 24-hour format.")
            return

        schedule.every().day.at(time_str).do(job)
        print(f"Scheduled ETL to run daily at {time_str}")

    elif choice == "2":
        hours = input("Enter interval in hours (positive integer): ").strip()
        if not hours.isdigit() or int(hours) <= 0:
            print("Invalid hours input. Please enter a positive integer.")
            return
        interval = int(hours)
        schedule.every(interval).hours.do(job)
        print(f"Scheduled ETL to run every {interval} hour(s)")

    else:
        print("Invalid choice. Please enter 1 or 2.")
        return

    print("Scheduler started. Press Ctrl+C to exit.")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
