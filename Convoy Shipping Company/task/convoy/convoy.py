import pandas as pd
import re as rg
import sqlite3
import json
from lxml import etree


def db(dbfile):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    return cur, con


def tbl_init(cur, con):
    sql = """CREATE TABLE IF NOT EXISTS convoy(
            vehicle_id INTEGER PRIMARY KEY NOT NULL,
            engine_capacity INTEGER NOT NULL,
            fuel_consumption INTEGER NOT NULL,
            maximum_load INTEGER NOT NULL)
    ;"""
    try:
        cur.execute('DELETE FROM convoy;')
    except Exception:
        cur.execute(sql)
    finally:
        con.commit()


def insert_tbl(table, cur, con, dbfile):
    alter = "ALTER TABLE convoy ADD COLUMN score INTEGER NOT NULL;"
    cur.execute(alter)
    sql = """
    INSERT INTO convoy(
            vehicle_id,
            engine_capacity,
            fuel_consumption,
            maximum_load, score)
            VALUES (?, ?, ?, ?, ?)
        """
    r, c = table.shape
    count = 0
    for row in range(r):
        line =[]
        for col in range(c):
            line.append(int(table.iat[row, col]))
        cur.execute(sql, line)
        count += cur.rowcount
    con.commit()
    con.close()
    log_line('{} record{} inserted into {}',
             count, "s were" if count > 1 else " was",
             dbfile)


def get_name():
    while True:
        in_file = input("Input file name\n")
        try:
            open(in_file, 'r')
        except Exception:
            print(f"Could not find {in_file}. Try again")
        else:
            base, ext = in_file.split('.')
            return base, ext


def get_table(base, ext):
    out_csv = base + '.csv'
    if  rg.match('xlsx', ext):
        table = pd.read_excel(f'{base}.{ext}', sheet_name='Vehicles', dtype=str)
        table.to_csv(out_csv, index=0, header=True, mode='w')
    else:
        table = pd.read_csv(f'{base}.{ext}')
    return table


def laundry(table):
    counter = 0
    r, c = table.shape
    for col in range(c):
        for row in range(r):
            i = table.iat[row, col]
            if not rg.match('^\d+$', i):
                counter = counter + 1
                table.iloc[row, col] = ''.join([k for k in i if rg.match('\d',k)])
    return table, counter


def to_json(table, counter, json_file):
    with open(json_file, 'w') as f:
        json.dump(table, f)
    log_line('{} vehicle{} saved into {}',
                   counter,
                   "s were" if counter == 0 or counter > 1 else " was",
                   json_file)


def to_xml(table, counter, xml_file):
    root = etree.Element('convoy')
    root.text = ''
    for vehicles in table['convoy']:
        v_tag = etree.SubElement(root, 'vehicle')
        for (k, v) in vehicles.items():
            k_tag = etree.SubElement(v_tag, k)
            k_tag.text = str(v)
    tree = etree.ElementTree(root)
    tree.write(xml_file)
    log_line('{} vehicle{} saved into {}',
                   counter,
                   "s were" if counter == 0 or counter > 1 else " was",
                   xml_file)

def from_db(dbfile, filter):
    sql_json = """
    SELECT 
            vehicle_id,
            engine_capacity,
            fuel_consumption,
            maximum_load
    FROM convoy 
    WHERE score > 3
    """
    sql_xml = """
    SELECT 
            vehicle_id,
            engine_capacity,
            fuel_consumption,
            maximum_load
    FROM convoy 
    WHERE score <= 3;
    """
   # sql_all = "SELECT *     FROM convoy;"
    if filter == 'json':
        sql = sql_json
    else:
        sql = sql_xml
    con = db(dbfile)[1]
    con.row_factory = sqlite3.Row
    rows = con.execute(sql).fetchall()
    table = {'convoy': []}
    for row in rows:
        table['convoy'].append({k : row[k] for k in row.keys() })
    return table, len(rows)


def to_csv(table, counter, base, ext):
    out_csv = f"{base}[CHECKED].csv"
    line_count = table.shape[0]
    table.to_csv(out_csv, index=0, header=True, mode='w')
    if ext == 'xlsx':
        log_line('{} line{} added ot to {}.csv',
                       line_count,
                       "s were" if line_count == 0 or line_count > 1 else " was",
                       base)
    log_line('{} cell{} corrected in {}',
                   counter,
                   "s were" if counter == 0 | counter > 1 else " was",
                   out_csv)

def log_line(fline, count, cond, outer):
    print(f"{fline}".format(count, cond, outer))


def score(table, average_route=450,burned_fuel=230, capacity=20):
    convoy = {}
    i = 0
    for vehicle in table.values():
        score = 0
        route = int(vehicle['engine_capacity']) / int( vehicle['fuel_consumption']) * 100
        fuel_consumption = average_route / 100 * int(vehicle['fuel_consumption'])
        if route >= average_route:
            score += 2
        elif route < average_route:
            if 2 * route >= average_route:
                score += 1
        if fuel_consumption <= burned_fuel:
            score += 2
        else:
            score += 1
        if int(vehicle['maximum_load']) >= capacity:
            score += 2
        vehicle['score'] = score
        convoy[i] = vehicle
        i += 1
    return pd.DataFrame.from_dict(convoy, orient='index')


def main():
    base, ext = get_name()
    json_file = f"{base.rstrip('[CHECKED]')}.json"
    xml_file = f"{base.rstrip('[CHECKED]')}.xml"
    dbfile = base.rstrip('[CHECKED]') + '.s3db'
    if rg.match('csv|xlsx', ext):
        table = get_table(base, ext)
        if not rg.match(".+\[CHECKED\]", base):
            table, counter = laundry(table)
            to_csv(table, counter, base, ext)
        cur, con = db(dbfile)
        tbl_init(cur, con)
        table = score((pd.DataFrame.to_dict(table, orient='index')))
        insert_tbl(table, cur, con, dbfile)
    elif rg.match('s3db', ext):
        dbfile = f'{base}.s3db'
    json_table, json_counter = from_db(dbfile, 'json')
    xml_table, xml_counter = from_db(dbfile, 'xml')
    to_json(json_table, json_counter, json_file)
    to_xml(xml_table, xml_counter, xml_file)


if __name__ == "__main__":
    main()
