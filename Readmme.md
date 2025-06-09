# ETL and Regression Analysis Pipeline

This project provides an end-to-end ETL and regression analysis pipeline. It includes dataset downloading, preprocessing, database integration, scheduled ETL execution, and data analysis using regression models.

## Project Structure

```
├── pipeline.py       # Handles dataset download, preprocessing, and loading into SQLite database
├── run.py            # Schedules the ETL pipeline execution
├── main.ipynb        # Performs data analysis and regression modeling
├── requirements.txt  # Python dependencies
```

## Usage

### Step 1: Run the ETL pipeline
Executes the full pipeline: downloads the dataset, preprocesses the data, and loads it into a local SQLite database.
```bash
python pipeline.py
```

### Step 2: Schedule the ETL job
Schedules the pipeline to run periodically using the `schedule` module.
```bash
python run.py
```

### Step 3: Perform data analysis and regression modeling
Open the Jupyter Notebook and run each cell to explore and model the data.
```bash
jupyter notebook main.ipynb
```

## Requirements

The `requirements.txt` file includes all necessary packages:
- pandas
- numpy
- matplotlib
- scikit-learn
- schedule

## Database

The ETL process stores the cleaned dataset in a local SQLite database (`.db` file) in `data` folder.

## Notes

- Ensure internet access is available when running `pipeline.py` for dataset download.
- Modify the scheduling interval in `run.py` according to your desired frequency.

