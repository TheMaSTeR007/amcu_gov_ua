from amcu_gov_ua.spiders.amcu_gov_ukraine import remove_diacritics, remove_punctuation, remove_extra_spaces, set_na
from doctor_trans import trans
import pandas as pd
import sys


def df_cleaner_title_also(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters

        # TODO: Cleaning 'title' column here as the native text gives different text after translation, cleaning the native file and rewriting again.
        if 'title' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


if __name__ == '__main__':
    # Get the filenames from command-line arguments
    if len(sys.argv) < 3:
        print("Usage: python translate_and_save.py <native_excel_file> <translated_excel_file>")
        sys.exit(1)

    print("Creating Translated sheet...")
    filename_native = sys.argv[1]  # The first argument is the native file path
    filename_translated = sys.argv[2]  # The second argument is the translated file path
    source_lang = sys.argv[3]  # The third argument is the native language code

    # Read Native Excel file
    native_data_df = pd.read_excel(io=filename_native, engine='calamine')
    native_data_df.drop(columns='id', axis=1, inplace=True)  # Drop 'id' column from native_df

    # NOTE: Since some urls are getting error 404 due to change in translation process, removing and adding detail_page_url explicitely
    # Step 1: Extract 'detail_page_url' column and store it separately
    detail_page_url_column = native_data_df['detail_page_url']  # Save the column
    detail_page_url_index = native_data_df.columns.get_loc('detail_page_url')  # Get the original index of the column
    native_data_df.drop(columns=['detail_page_url'], inplace=True)  # Remove the column

    # Step 2: Translate the DataFrame to English and return translated DataFrame
    tranlated_df = trans(native_data_df, input_lang=source_lang, output_lang='en')  # Change the input-lang when required

    # Step 3: Clean the translated DataFrame
    cleaned_tranlated_df = df_cleaner_title_also(data_frame=tranlated_df)  # Apply the function to all columns for Cleaning
    cleaned_native_df = df_cleaner_title_also(data_frame=native_data_df)  # Apply the function to all columns for Cleaning

    # Step 4: Reinsert 'detail_page_url' column back into the DataFrame at its original position
    cleaned_tranlated_df.insert(loc=detail_page_url_index, column='detail_page_url', value=detail_page_url_column)
    cleaned_native_df.insert(loc=detail_page_url_index, column='detail_page_url', value=detail_page_url_column)

    # Write translated_df in Excel file
    try:
        print("Creating Translated sheet... in translate_and_save.py")
        with pd.ExcelWriter(path=filename_translated, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
            cleaned_tranlated_df.insert(loc=0, column='id', value=range(1, len(cleaned_tranlated_df) + 1))  # Add 'id' column at position 1
            cleaned_tranlated_df.to_excel(excel_writer=writer, index=False)
            print("Translated Excel file Successfully created!  in translate_and_save.py")
    except Exception as e:
        print(f'Error while Generating Translated Excel file:  in translate_and_save.py {e}')

    # Write native_df in Excel file
    try:
        print("Creating Native sheet... in translate_and_save.py")
        with pd.ExcelWriter(path=filename_native, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
            cleaned_native_df.insert(loc=0, column='id', value=range(1, len(cleaned_native_df) + 1))  # Add 'id' column at position 1
            cleaned_native_df.to_excel(excel_writer=writer, index=False)
        print("Native Excel file Successfully created! in translate_and_save.py")
    except Exception as e:
        print(f'Error while Generating Native Excel file: in translate_and_save.py {e}')
