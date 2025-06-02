# data_cleaning_task.py
import pandas as pd
from datetime import datetime
from io import StringIO

def clean_column_names(df):
    """Clean column names to be lowercase with underscores"""
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def clean_data(df):
    """Perform all cleaning operations on the dataframe"""
    # First clean column names
    df = clean_column_names(df)
    
    # 1. Clean and handle missing values in Age
    if 'age' in df.columns:
        # Remove any whitespace and convert empty strings to NaN
        df['age'] = df['age'].astype(str).str.strip()
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        df['age'] = df['age'].fillna(df['age'].median()).astype(int)
    
    # 2. Clean and handle missing values in Purchase Amount
    purchase_col = next((col for col in df.columns if 'purchase' in col.lower()), None)
    if purchase_col:
        df[purchase_col] = pd.to_numeric(df[purchase_col], errors='coerce')
        df[purchase_col] = df[purchase_col].fillna(df[purchase_col].mean())
    
    # 3. Clean Active Member column
    member_col = next((col for col in df.columns if 'active' in col.lower()), None)
    if member_col:
        df[member_col] = df[member_col].astype(str).str.strip().str.lower()
        active_mode = df[member_col].mode()[0] if not df[member_col].mode().empty else 'yes'
        df[member_col] = df[member_col].fillna(active_mode)
        df[member_col] = df[member_col].map({'yes': True, 'no': False, 'true': True, 'false': False})
    
    # 4. Clean Join Date
    date_col = next((col for col in df.columns if 'date' in col.lower()), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # 5. Standardize text data
    if 'gender' in df.columns:
        df['gender'] = df['gender'].astype(str).str.strip().str.lower()
        gender_map = {'m': 'male', 'male': 'male', 'f': 'female', 'female': 'female'}
        df['gender'] = df['gender'].map(gender_map)
    
    if 'country' in df.columns:
        df['country'] = df['country'].astype(str).str.strip().str.title()
    
    if 'first_name' in df.columns:
        df['first_name'] = df['first_name'].astype(str).str.strip().str.title()
    
    if 'last_name' in df.columns:
        df['last_name'] = df['last_name'].astype(str).str.strip().str.title()
    
    # 6. Remove duplicates
    duplicate_cols = [col for col in ['first_name', 'last_name', 'age', 'country'] if col in df.columns]
    if duplicate_cols:
        df = df.drop_duplicates(subset=duplicate_cols, keep='first')
    
    return df

def generate_report(original_df, clean_df):
    """Generate a cleaning report"""
    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'original_rows': len(original_df),
        'cleaned_rows': len(clean_df),
        'duplicates_removed': len(original_df) - len(clean_df),
        'columns_cleaned': list(clean_df.columns),
    }
    return report

def save_outputs(clean_df, report):
    """Save cleaned data and report to files"""
    clean_df.to_csv('cleaned_customer_data.csv', index=False)
    
    with open('cleaning_report.txt', 'w') as f:
        f.write("DATA CLEANING REPORT\n")
        f.write("===================\n\n")
        f.write(f"Cleaning performed at: {report['timestamp']}\n\n")
        f.write(f"Original rows: {report['original_rows']}\n")
        f.write(f"Cleaned rows: {report['cleaned_rows']}\n")
        f.write(f"Duplicates removed: {report['duplicates_removed']}\n\n")
        f.write("Columns in cleaned data:\n")
        for col in report['columns_cleaned']:
            f.write(f"- {col}\n")
    
    print("Cleaning complete! Output files saved:")
    print("- cleaned_customer_data.csv")
    print("- cleaning_report.txt")

def main():
    # Raw data string with fixed whitespace issues
    raw_data = """ID,First Name,last name,Age,Gender,Country,Join Date,Purchase Amount ($),Active Member
1,John,Doe,28,M,USA,12/05/2020,150.50,Yes
2,Jane,Smith,35,F,Canada,2020-07-15,200.75,yes
3,michael,Johnson,,Male,UK,15-08-2021,300.00,NO
4,Sarah,Williams,42,f,Australia,2021/09/20,175.25,Yes
5,David,Brown,31,M,USA,10-11-2020,,Yes
6,jane,smith,35,female,canada,2020-07-15,200.75,yes
7,Emily,Davis,29,F,Germany,2022-01-05,225.50,
8,Robert,Wilson,50,m,France,12/03/2021,275.00,No
9,Lisa,Taylor,38,F,USA,2021-04-18,190.75,yes
10,James,Anderson,45,Male,UK,20-05-2020,310.00,YES
11,Jessica,Thomas,33,F,Canada,2022-02-14,,Yes
12,Lisa,Taylor,38,f,usa,2021-04-18,190.75,yes
13,Daniel,Martinez,27,M,Spain,2021-07-22,240.50,no
14,amy,Robinson,41,Female,Germany,2022-03-10,265.25,Yes
15,Matthew,White,36,M,France,,280.00,No"""
    
    # Create DataFrame from the raw data string
    df = pd.read_csv(StringIO(raw_data))
    
    # Perform cleaning
    cleaned_df = clean_data(df.copy())
    
    # Generate report
    cleaning_report = generate_report(df, cleaned_df)
    
    # Save outputs
    save_outputs(cleaned_df, cleaning_report)

if __name__ == "__main__":
    main()