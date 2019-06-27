# Comment by Neville
python_addin_version = '0.0.6'
import xlwings as xw
import win32api
import requests
import collections
import datetime
import json
import sys
import pandas as pd
import numpy as np
import traceback
import os
from dateutil import parser
from rose_wrapper.rose import Rose
import subprocess
import ast

from decimal import Decimal

rose = Rose()
rose.base_url = 'https://rose.ai'
rose_username = os.environ['ROSE_USERNAME']
rose_password = os.environ['ROSE_PASSWORD']

try:
    bbg_username = os.environ['BBG_USERNAME']
    bbg_password = os.environ['BBG_PASSWORD']
    rose.login(bbg_username, bbg_password)
    bbg_connected = True
except:
    bbg_connected = False

try:
    rose.login(rose_username, rose_password)
    connected = True
except requests.ConnectionError:
    connected = False

debug = 1

invalid_excel_values = ['', 'None', '#N/A', -2146826281, -2146826245, -2146826246, -2146826259, -2146826288, -2146826252, -2146826265, -2146826273]
invalid_excel_values_str = [str(c) for c in invalid_excel_values]

def _merge_left_in_order(x, y, on=None):
    x = x.copy()
    x["Order"] = np.arange(len(x))
    z = x.merge(y, how='left', left_index=True, right_index=True).set_index("Order").ix[np.arange(len(x)), :]
    return z

def rose_version(vba_addin_version):
    versions = pd.DataFrame(data={'type':['Excel Addin','Python Addin'],'versions':[str(vba_addin_version), python_addin_version]})
    xw.Range((2, 1)).options(header=False, index=False).value = versions

def pull(args=None):

    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    codes = args[0]
    as_range = bool(int(args[1]))
    active_row = int(args[2])
    active_column = int(args[3])

    if active_column < 2:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "'Pull' cannot be in the first column")

    try:
        errors = None
        if as_range:
            code_range = xw.Range(codes)
            codes = [{'value': str(int(code.value)) if isinstance(code.value, (float, int)) else code.value, 'row': code.row, 'column': code.column}
                     for code in code_range if code.value is not None]
        else:
            codes = [{'value': str(codes), 'row': active_row, 'column': active_column}]
        if len(args) == 5:
            errors = pull_sub(codes, active_row, active_column, args[4])
        else:
            errors = pull_sub(codes, active_row, active_column)
        if len(errors) > 0:
            win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors\n" + "\n".join(errors))
        else:
            win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd, str(traceback.format_exc()) if debug == 1 else "Unknown error")


def pull_sub(codes, active_row, active_column, date_address=None):
    errors = []

    if date_address is None:
        values_df = pd.DataFrame()
        metas_df = pd.DataFrame()

        for code in codes:
            try:
                dataset_df, dataset_metas_df, dataset_values_df = rose.pull(code['value'], as_pandas=True, exact_match=False)
                dataset_values_df = dataset_values_df.rename(columns={'value': code['value']})
                dataset_values_index_list = list(dataset_values_df.index.strftime('%Y-%m-%d'))
                dataset_values_df.index = [str(dateindex[:10].replace('-','.')) if int(dateindex.split('-')[0])<=1899 else dateindex for dateindex in dataset_values_index_list]
                dataset_values_df.index = dataset_values_df.index.map(str)
                dataset_metas_df = dataset_metas_df.rename(columns={'value': code['value']})
                if 'tree' in dataset_metas_df.index:
                    dataset_metas_df = dataset_metas_df.drop('tree')

                values_df = values_df.merge(dataset_values_df, how='outer', left_index=True, right_index=True)
                metas_df = metas_df.merge(dataset_metas_df, how='outer', left_index=True, right_index=True)
            except requests.exceptions.RequestException as e:
                try:
                    response_json = e.response.json()
                    message = response_json['message']
                except:
                    message = 'Unknown Error'
                error_message = str(traceback.format_exc()) if debug == 1 else "Error for " + str(code['value']) + ": " + message
                errors.append(error_message)
                value_df = pd.DataFrame(columns = [code['value']])
                meta_df = pd.DataFrame(columns = [code['value']])
                values_df = values_df.merge(value_df, how='outer', left_index=True, right_index=True)
                metas_df = metas_df.merge(meta_df, how='outer', left_index=True, right_index=True)
                continue
            except:
                error_message = str(traceback.format_exc()) if debug == 1 else "Error for " + str(code['value']) + ": " + "Unknown Error"
                errors.append(error_message)
                value_df = pd.DataFrame(columns = [code['value']])
                meta_df = pd.DataFrame(columns = [code['value']])
                values_df = values_df.merge(value_df, how='outer', left_index=True, right_index=True)
                metas_df = metas_df.merge(meta_df, how='outer', left_index=True, right_index=True)
                continue

        if values_df is not None:
            values_df = values_df.fillna("#N/A")
            values_df.index = values_df.index.map(str)

            xw.Range((codes[0]['row'] + 1, codes[0]['column'] - 1)).options(header=False).value = pd.DataFrame(index=metas_df.index)
            xw.Range((codes[0]['row'] + 1 + len(metas_df) + 1, codes[0]['column'] - 1)).options(header=False).value = pd.DataFrame(index=values_df.index)

            for column in values_df.columns:
                for code in codes:
                    if code['value'] == column:
                        xw.Range((code['row'] + 1, code['column'])).options(header=False, index=False).value = metas_df[column]
                        xw.Range((code['row'] + 1 + len(metas_df) + 1, code['column'])).options(header=False, index=False).value = values_df[column]

    if date_address is not None:
        meta_tag_range = xw.Range(xw.Range((1, 1)), xw.Range((active_row - 1, 1)))
        date_range = xw.Range(date_address)
        date_range_values = []
        for d in date_range.value:
            try:
                date_range_values.append(parser.parse(d).date())
            except:
                date_range_values.append(d.date())

        values_df = pd.DataFrame(index=date_range_values)
        metas_df = pd.DataFrame(index=[str(tag).lower() if tag is not None else None for tag in meta_tag_range.value])
        for code in codes:
            metas_df['backup'] = meta_tag_range.offset(0, code['column'] - meta_tag_range.column).value
            dataset_values_df = pd.DataFrame(columns=['value'])
            dataset_metas_df = pd.DataFrame(columns=['value'])
            try:
                dataset_df, dataset_metas_df, dataset_values_df = rose.pull(code['value'], as_pandas=True, exact_match=False)
                dataset_values_df.index = [c.date() for c in dataset_values_df.index]
                dataset_metas_df.index = [str(tag).lower() for tag in dataset_metas_df.index]
                if 'tree' in dataset_metas_df.index:
                    dataset_metas_df = dataset_metas_df.drop('tree')
            except requests.exceptions.RequestException as e:
                try:
                    response_json = e.response.json()
                    message = response_json['message']
                except:
                    message = 'Unknown Error'
                error_message = str(traceback.format_exc()) if debug == 1 else "Error for " + str(code['value']) + ": " + message
                errors.append(error_message)
                dataset_values_df = pd.DataFrame(columns = ['value'])
                dataset_metas_df = pd.DataFrame(columns = ['value'])
            except:
                error_message = str(traceback.format_exc()) if debug == 1 else "Error for " + str(code['value']) + ": " + "Unknown Error"
                errors.append(error_message)
                dataset_values_df = pd.DataFrame(columns = ['value'])
                dataset_metas_df = pd.DataFrame(columns = ['value'])

            dataset_values_df = dataset_values_df.rename(columns={'value': code['value']})
            dataset_metas_df = dataset_metas_df.rename(columns={'value': code['value']})

            merged_values_df = _merge_left_in_order(values_df, dataset_values_df) if values_df is not None else dataset_values_df
            merged_metas_df = _merge_left_in_order(metas_df, dataset_metas_df) if metas_df is not None else dataset_metas_df

            merged_values_df = merged_values_df.fillna("#N/A")
            xw.Range((date_range.row, code['column'])).options(index=False, header=False).value = merged_values_df[code['value']]

            merged_metas_df['in_pull_metas'] = pd.notnull(merged_metas_df[code['value']])  # Tag all meta tags not in pull as is_null
            for idx, row in merged_metas_df.iterrows():
                if row['in_pull_metas']:
                    xw.Range((1 + int(idx), code['column'])).value = row[code['value']]
    return errors

def push(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    codes = args[0]
    as_range = bool(int(args[1]))
    active_row = int(args[2])
    active_column = int(args[3])
    file_location = args[5]
    file_saved =  "\\" in file_location or "/" in file_location

    if active_column < 2:
        return

    try:
        errors = None
        if as_range:
            code_range = xw.Range(codes)
            codes = [{'value': code.value, 'row': code.row, 'column': code.column} for code in code_range]
        else:
            codes = [{'value': codes, 'row': active_row, 'column': active_column}]
        errors = push_sub(codes, active_row, active_column, args[4], args[5])

        if len(errors) > 0:
            win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors\n" + "\n".join(errors))
        else:
            win32api.MessageBox(xw.Book.caller().app.hwnd, str("Success") if file_saved else "Success, but file_location is not stored in metadata because your excel file is not saved")
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd, str(traceback.format_exc()) if debug == 1 else "Unknown error occured")

def push_sub(codes, active_row, active_column, date_address=None, file_location = None):
    date_range = None
    meta_tag_range = xw.Range(xw.Range((1, 1)), xw.Range((active_row - 1, 1)))
    errors = []

    if date_address == '0':
        date_range = xw.Range(xw.Range((active_row + 1, 1)), xw.Range((active_row + 1, 1)).end('down'))
    else:
        date_range = xw.Range(date_address)

    for code in codes:
        try:
            if active_row == 2:
                metas_df = pd.DataFrame({})
            else:
                metas_df = pd.DataFrame(data={'value': meta_tag_range.offset(0, code['column'] - meta_tag_range.column).value}, index=meta_tag_range.value).dropna()
            if len(metas_df.index)!=len(set(metas_df.index)):
                raise Exception('Metadata table keys are not unique')
                break
            values_df = pd.DataFrame(data={'value': date_range.offset(0, code['column'] - date_range.column).value}, index=date_range.value).dropna()
            values_df = values_df[~values_df.value.isin(invalid_excel_values)]
            try:
                metas_df = metas_df.loc[metas_df.index.dropna()]
            except:
                raise Exception('Metadata table keys are not unique')
                break
            if file_location is not None:
                metas_df.loc['file_location', 'value'] = file_location

            rose.push(code=code['value'], metas=metas_df, values=values_df)
        except requests.exceptions.RequestException as e:
            error_message = str(traceback.format_exc()) if debug == 1 else (code['value'] + " cannot be pushed: " + str(e.response.json()['message']))
            errors.append(error_message)
        except:
            error_message = str(code) + " code failed\n" + str(traceback.format_exc())
            errors.append(error_message)

    return errors
def pull_map(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    code = args[0]
    active_row = int(args[1])
    active_column = int(args[2])

    columns_exist = bool(int(args[3]))
    column_list = args[4]

    try:
        _, metas, values_df = rose.pull(code, as_pandas=True)

        if 'column_order' in metas.index:
            column_order = ast.literal_eval(metas.loc['column_order'].value)
            values_df = values_df[column_order]

        if column_list:
            new_values_df = pd.DataFrame()
            for column in xw.Range(column_list):
                new_values_df = new_values_df.merge(values_df[[column.value]], how='outer', right_index=True, left_index=True)
            values_df = new_values_df

        xw.Range((active_row + 1, active_column)).options(index=False, header=True).value = values_df
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd, str(traceback.format_exc()))

def push_map(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    code = args[0]
    map_range = args[1]
    meta_range = args[2]
    if meta_range == '0':
        metas_df = pd.DataFrame(columns=['tag', 'attribute'])
        metas_df.set_index('tag', inplace=True)
    else:
        metas_df = xw.Range(meta_range).options(pd.DataFrame,header = False, index = True).value.reset_index()
        metas_df = metas_df.dropna().set_index('index')
        metas_df.columns = ['value']
        if len(metas_df.index)!=len(set(metas_df.index)):
            win32api.MessageBox(xw.Book.caller().app.hwnd,'Error: Metadata table keys are not unique')
            return

    rose_map = xw.Range(map_range).options(pd.DataFrame).value.reset_index()
    rose_map.columns = rose_map.columns.get_level_values(0)

    rose_map = rose_map.where((pd.notnull(rose_map)), None)
    rose_map.columns = rose_map.columns.astype(str)

    for column in rose_map.columns:
        for index in rose_map[column].index:
            if type(rose_map.loc[index, column]) == datetime.datetime:
                rose_map.loc[index, column] = str(rose_map.loc[index, column])
            if isinstance(rose_map.loc[index, column], Decimal):
                rose_map.loc[index, column] = float(rose_map.loc[index, column])
            # rose_map.loc[index, column] = unicode(rose_map.loc[index, column])
            try:
                if str(rose_map.loc[index, column]) in invalid_excel_values_str:
                    rose_map.loc[index, column] = ''
            except:
                win32api.MessageBox(xw.Book.caller().app.hwnd, "Error reading value " + str(rose_map.loc[index, column]) + " in column " + str(column) + ": Check to make sure that the column doesn't contain null values and all the rows in the column have the same data type.")
                break
    try:
        rose.push(code=code,  metas=metas_df, values=rose_map, data_type='map')
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd, str(traceback.format_exc()) if debug == 1 else "Unknown error occured")

def pull_logic(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    rose_code_range = args[0]
    rose_code_as_range = bool(int(args[1]))
    active_row = int(args[2])
    active_column = int(args[3])

    rose_codes = None
    if rose_code_as_range:
        rose_codes = [str(cell.value) for cell in xw.Range(rose_code_range)]
    else:
        rose_codes = [rose_code_range]

    rose_code_logic_df = pd.DataFrame(index = rose_codes)
    errors = []

    for rose_code in rose_codes:
        if rose_code in invalid_excel_values:
            rose_code_logic_df.loc[rose_code, 'logic'] = ''
        else:
            try:
                rose_code_json = rose.pull_logic(rose_code)
                rose_code_logic_df.loc[rose_code, 'logic'] = rose_code_json['logic']
            except requests.exceptions.RequestException as e:
                errors.append("Error (" + rose_code + "): " + (str(traceback.format_exc()) if debug == 1 else str(e.response.json()['message'])))

    if rose_code_as_range:
        xw.Range((xw.Range(rose_code_range).row, xw.Range(rose_code_range).column + 1)).options(header=False, index=False).value = rose_code_logic_df
    else:
        xw.Range((active_row, active_column + 1)).options(header=False, index=False).value = rose_code_logic_df

    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Finished with errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def push_logic(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    rose_code_range = args[0]
    rose_code_as_range = bool(int(args[1]))
    logic_range = args[2]
    logic_as_range = bool(int(args[3]))

    if rose_code_as_range:
        rose_codes = [str(cell.value) for cell in xw.Range(rose_code_range)]
    else:
        rose_codes = [rose_code_range]

    logics = None
    if logic_as_range:
        logics = [str(cell.value) for cell in xw.Range(logic_range)]
    else:
        logics = [logic_range]

    if len(logics) != len(rose_codes):
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Num logics doesn't equal num rose_codes")
        return

    errors = []
    bblist = ['GOVT','CORP','MTGE','CRNCY','CURNCY', 'EQUITY','INDEX', 'CMDTY','COMDTY']
    for idx, logic in enumerate(logics):
        if rose_codes[idx] in invalid_excel_values or logics[idx] in invalid_excel_values:
            continue
        try:
            rose.push_logic(code=rose_codes[idx], logic=logics[idx])
        except:
            rose_code = rose_codes[idx]
            if (len(logics[idx].split('.')) > 1) & logics[idx][logics[idx].rfind('.')+1].isalpha():
                bbg_code = '.'.join(logics[idx].split('.')[:-1])
                field = logics[idx].split('.')[-1]
            else:
                bbg_code = logics[idx]
                field = 'PX_LAST'
            if any(word.lower() in [b.lower() for b in bblist] for word in bbg_code.split(' ')):
                errorsbbg = push_bbg_to_rose_sub([bbg_code], [rose_code], field)
                if len(errorsbbg) > 0:
                    if debug == 1:
                        for e in errorsbbg:
                            errors.append(e)
                    else:
                        errors.append("Rose can't find the bloomberg ticker you have provided. Please confirm the code exists in bloomberg before trying to push again.")
            elif '@' in logics[idx]:
                haver_code = logics[idx].lower().replace('@', '.')
                command_to_run = 'py haver-upload.py ' + haver_code
                exit_code = subprocess.call(command_to_run, shell=True, cwd=os.path.dirname(os.path.realpath(__file__)))
                if exit_code > 0:
                    errors.append("Error (" + rose_codes[idx] + "): " + "failed to pull data from Haver")
                try:
                    rose.push_logic(code=rose_codes[idx], logic=haver_code)
                except requests.exceptions.RequestException as e:
                    errors.append("Error (" + rose_codes[idx] + "): " + (str(traceback.format_exc()) if debug == 1 else str(e.response.json()['message'])))
            else:
                errors.append("Error (" + rose_codes[idx] + "): " + (str(traceback.format_exc()) if debug == 1 else "Could not determine the source of the logic you would like to push to Rose. Currently, Rose can detect Bloomberg Tickers or Haver IDs"))

    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def update(args):
    if not connected:
        return win32api.MessageBox(xw.Book.caller().app.hwnd, "Error: Internet connection failed")
    code_range = args[0]
    code_as_range = bool(int(args[1]))

    codes = None
    if code_as_range:
        codes = [str(cell.value) for cell in xw.Range(code_range)]
    else:
        codes = [code_range]

    errors = []

    anytree_exists = False
    try:
        from anytree import PreOrderIter
        from anytree.importer import DictImporter
        anytree_exists = True
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Couldn't find python library: anytree. Update codes will continue but would run better with anytree installed. To install anytree, open command prompt and type 'pip install anytree'")

    for idx, code in enumerate(codes):
        if code == 'None':
            continue
        if anytree_exists:
            try:
                dataset = rose.pull(code, exact_match=False)
                tree = dataset['metas']['tree']
            except:
                try:
                    find_and_update_code(code)
                except:
                    errors.append("Unknown Error Getting Code (" + code + "): " + str(traceback.format_exc()) if debug == 1 else code)
                continue

            importer = DictImporter()
            current_node = importer.import_(tree)
            leaves = [node.code for node in PreOrderIter(current_node) if node.is_leaf]
            for leaf in leaves:
                try:
                    dataset = rose.pull(leaf)
                    if dataset['actor'].lower() == 'bbg':
                        ticker = dataset['metas']['TICKER'] if 'TICKER' in dataset['metas'] else dataset['code'].split('.')[0]
                        field = dataset['code'].split('.')[1]
                        errors = push_bbg_to_rose_sub([ticker], field=field)
                        if len(errors) > 0:
                            raise Exception("Unknown error")

                    elif dataset['actor'].lower() == 'haver':
                        cleaned_code = dataset['code'].lower().replace('@', '.')
                        command_to_run = 'py haver-upload.py ' + cleaned_code
                        exit_code = subprocess.call(command_to_run, shell=True, cwd=os.path.dirname(os.path.realpath(__file__)))
                        if exit_code > 0:
                            raise Exception(exit_code)
                    else:
                        raise Exception('Code cannot be auto-updated')
                except:
                    errors.append("Cannot update " + code + ". Underlying database not found\n" + str(traceback.format_exc()) if debug == 1 else 'leaf: '+ leaf)
            try:
                rose.delete_from_cache(code)
            except:
                errors.append("Cannot remove cache for: " + code)
        else:
            try:
                find_and_update_code(code)
            except:
                errors.append("Unknown Error Getting Code (" + code + "): " + str(traceback.format_exc()) if debug == 1 else '')


    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def find_and_update_code(code):

    bblist = ['GOVT','CORP','MTGE','CRNCY','CURNCY', 'EQUITY','INDEX', 'CMDTY','COMDTY']
    if len(code.split('.'))>1:
        #bbg_code = '.'.join(code.split('.')[:-1])
        bbg_code = ('.'.join(code.split('.')[:-1])).replace('_',' ')
        field = code.split('.')[-1]
    else:
        bbg_code = code
        field = 'PX_LAST'
    if any(word.lower() in [b.lower() for b in bblist] for word in bbg_code.split(' ')):
        errorsbbg = push_bbg_to_rose_sub([bbg_code], [], field)
        if len(errorsbbg) > 0:
            raise Exception("Unknown error")
    elif '@' in code or code.count('.') == 1:
        haver_code = code.lower().replace('@', '.')
        command_to_run = 'py haver-upload.py ' + haver_code
        exit_code = subprocess.call(command_to_run, shell=True, cwd=os.path.dirname(os.path.realpath(__file__)))
        if exit_code > 0:
            raise Exception(exit_code)
    else:
        raise Exception('Code not recognized')

def push_bbg_to_rose(args):
    ticker_range = args[0]
    ticker_as_range = bool(int(args[1]))
    rose_code_range = args[2]
    rose_code_as_range = bool(int(args[3]))
    field = args[4]
    start_date = parser.parse(args[5])
    freq = args[6]

    tickers = None
    if ticker_as_range:
        tickers = [str(cell.value) for cell in xw.Range(ticker_range)]
    else:
        tickers = [ticker_range]

    if rose_code_as_range:
        rose_codes = [str(cell.value) for cell in xw.Range(rose_code_range)]
    else:
        rose_codes = [rose_code_range]

    errors = push_bbg_to_rose_sub(tickers, rose_codes, field, start_date, freq)
    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def push_bbg_to_rose_sub(tickers, rose_codes=[], field='PX_LAST', start_date=datetime.datetime(1960, 1, 1), freq='DAILY'):
    if not bbg_connected:
        return(['Bloomburg not connected. Make sure you have bbg Rose account info in environmental variables'])
    
    try:
        from rose_wrapper.bbg import simpleHistoryRequest, simpleReferenceDataRequest
    except:
        return ["Bloomberg API not installed" if debug == 0 else str(traceback.format_exc())]

    rose_bbg = Rose()
    rose_bbg.base_url = 'https://rose.ai'
    rose_bbg.login(bbg_username, bbg_password)

    rose_codes_exist = True
    if len(rose_codes) == 0 or (len(rose_codes) == 1 and rose_codes[0] == ""):
        rose_codes_exist = False

    if rose_codes_exist and len(tickers) != len(rose_codes):
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Num tickers doesn't equal num rose_codes")
        return

    errors = []
    for idx, rose_bbg_ticker in enumerate(tickers):
        ticker = rose_bbg_ticker
        try:
            bbg_data_values = simpleHistoryRequest([ticker], [field], periodicity=freq, startDate=start_date, endDate=datetime.datetime(2050, 1, 1)).dropna()
            bbg_data_values.columns = bbg_data_values.columns.droplevel(1)

            meta_search_df = {}
            meta_search_df[ticker] = ticker
            bbg_data_metas = simpleReferenceDataRequest(meta_search_df, ['NAME', 'LONG_COMP_NAME', 'UNITS', 'SECURITY_DES', 'QUOTE_UNITS', 'FUND_EXPENSE_RATIO', 'COUNTRY', 'CURRENCY', 'CONTRACT_VALUE',
                                                                         'INDUSTRY_SECTOR', 'INDUSTRY_GROUP', 'GICS_SECTOR_NAME', 'GICS_INDUSTRY_NAME', 'GICS_INDUSTRY_GROUP_NAME', 'GICS_SUB_INDUSTRY_NAME', 'MODIFIED_DURATION']).transpose().dropna()

            bbg_data_metas.loc['TICKER'] = ticker
            if bbg_data_values[[ticker]].empty == False:
                rose_bbg.push(code=ticker.replace(" ", "_").replace('.', '_') + "." + field.replace(" ", "_").replace('.', '_'), metas=bbg_data_metas[[ticker]], values=bbg_data_values[[ticker]])
            else:
                raise Exception(ticker + " not found on BBG")

        except Exception as e:
            errors.append("Unknown Error Creating Dataset (" + ticker.replace(" ", "_").replace('.', '_') + "." +
                          field.replace(" ", "_").replace('.', '_') + "): " + str(traceback.format_exc()) if debug == 1 else ticker)

    if rose_codes_exist:
        for idx, rose_bbg_ticker in enumerate(tickers):
            ticker = rose_bbg_ticker.split(':')[0]
            logic = ticker.replace(" ", "_").replace('.', '_') + "." + field.replace(" ", "_").replace('.', '_')
            transformations = ':'.join(rose_bbg_ticker.split(':')[1:])
            logic = logic + (':' + transformations if len(transformations) > 0 else '')
            try:
                rose.push_logic(code=rose_codes[idx], logic=logic)
            except Exception as e:
                errors.append("Unknown Error Generating Rose Code (" + rose_codes[idx] + "): " + str(traceback.format_exc()) if debug == 1 else ticker)

    return errors

def push_yahoo_to_rose(args):
    try:
        import yfinance as yf
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd,"fix yahoo finance not installed, run 'pip install yfinance --upgrade --no-cache-dir'")
        return
    tickerList = args[0]
    tickerList_as_range = bool(int(args[1]))
    rosecodeList = args[2]
    rosecodeList_as_range = bool(int(args[3]))
    field = args[4].title()

    tickers = tickerList
    rosecodes = rosecodeList
    if tickerList_as_range:
        tickers = [str(cell.value) for cell in xw.Range(tickerList)]
    else:
        tickers = [tickerList]
    if rosecodeList_as_range:
        rosecodes = [str(cell.value) for cell in xw.Range(rosecodeList)]
    else:
        rosecodes = [rosecodeList]
    errors = push_yahoo_to_rose_sub(tickers, rosecodes,field)
    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def push_yahoo_to_rose_sub(tickers, rosecodes,field="Close"):
    import yfinance as yf

    rosecodes_exist= True
    if len(rosecodes) == 0 or (len(rosecodes) == 1 and rosecodes[0] == ""):
        rosecodes_exist = False
        win32api.MessageBox(xw.Book.caller().app.hwnd,"Need to assign rosecode to ticker")
        return
    if rosecodes_exist and len(tickers) != len(rosecodes):
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Number of tickers words doesn't equal num rosecodes")
        return
    for i, ticker in enumerate(tickers):
        upper = ticker.upper()
        df = yf.download(upper, start="1970-01-01", end=datetime.datetime.today().strftime('%Y-%m-%d'))
        yahoo_df = df[[field]]

        errors = []
        try:
            metas_values = ['yahoo',ticker,field,'b']
            metas_index = ['source','ticker','concept','frequency']
            metas_df = pd.DataFrame(metas_values,index = metas_index)
            rose.push(code=rosecodes[i], metas=metas_df, values=yahoo_df)
        except Exception as e:
            errors.append("Unknown Error Creating Dataset (" + rosecodes[i] + "): ")
    return errors

def push_trend_to_rose(args):
    try:
        from pytrends.request import TrendReq
    except:
        win32api.MessageBox(xw.Book.caller().app.hwnd,"pytrends package not installed, use 'pip install pytrends' to install Pytrends")
        return
    keyword_range = args[0]
    kw_list_as_range = bool(int(args[1]))
    rose_codes = args[2]
    rose_codes_as_range = bool(int(args[3]))

    kw_list = None
    if kw_list_as_range:
        kw_list = [str(cell.value) for cell in xw.Range(keyword_range)]
    else:
        kw_list = [code_range]

    if rose_codes_as_range:
        rose_codes = [str(cell.value) for cell in xw.Range(rose_codes)]
    else:
        rose_codes = [rose_codes]

    errors = push_trend_to_rose_sub(kw_list, rose_codes)
    if len(errors) > 0:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Errors for the following codes:\n" + "\n".join(errors))
    else:
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Success")

def push_trend_to_rose_sub(kw_list, rose_codes=[]):
    from pytrends.request import TrendReq
    rose_codes_exist = True
    if len(rose_codes) == 0 or (len(rose_codes) == 1 and rose_codes[0] == ""):
        rose_codes_exist = False

    if rose_codes_exist and len(kw_list) != len(rose_codes):
        win32api.MessageBox(xw.Book.caller().app.hwnd, "Num key words doesn't equal num rose_codes")
        return

    rose_trend = Rose()
    rose_trend.base_url = 'https://rose.ai'
    rose_trend.login('googletrends-upload@snow.ventures', 'bbrUW7qZb83s5PK')

    errors=[]
    pytrends = TrendReq(hl='en-US',tz=360)
    pytrends.build_payload(kw_list,cat=0,timeframe='all',geo='US',gprop='')
    trend_data_values = pytrends.interest_over_time()
    del trend_data_values['isPartial']

    errors = []
    for idx, column in enumerate(trend_data_values.columns):
        code = column.replace(' ', '_') + ".googletrends"
        try:
            metas_values = ['Google Trends',column]
            metas_index = ['source','Key word']
            metas_df = pd.DataFrame(metas_values,index = metas_index)
            rose_trend.push(code=code, metas=metas_df, values=trend_data_values[[column]])
            if rose_codes_exist:
                rose.push_logic(code=rose_codes[idx], logic=code)
        except Exception as e:
            errors.append("Unknown Error Creating Dataset (" + code + "): " + str(traceback.format_exc()) if debug == 1 else code)

    return errors
