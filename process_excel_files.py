import os
from openpyxl import load_workbook

def autofit_columns(sheet):
    """Adjust the width of columns based on the maximum length of the content."""
    for column in sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter  # Get the column letter (A, B, C, etc.)
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except Exception as e:
                print(f'Error reading cell {cell.coordinate}: {e}')
        adjusted_width = max_length + 2  # Adding some extra space
        sheet.column_dimensions[column_letter].width = adjusted_width

def rename_excel_sheets(directory, new_sheet_name):
    # Iterate through all files in the specified directory
    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            try:
                # Load the workbook and get the active sheet
                workbook = load_workbook(file_path)
                # Rename the first sheet
                first_sheet = workbook.worksheets[0]
                if first_sheet.title == 'Sheet1':
                    first_sheet.title = new_sheet_name
                    print(f'Renamed sheet in {filename} to "{new_sheet_name}"')

                [autofit_columns(sh) for sh in workbook.worksheets]
                # Save the workbook with the new sheet name
                workbook.save(file_path)
                
            except Exception as e:
                print(f'Failed to rename sheet in {filename}: {e}')

# Specify the directory and the new sheet name
directory_path = 'final_excel_to_send'
new_sheet_name = 'DirectCertUnknown'

rename_excel_sheets(directory_path, new_sheet_name)