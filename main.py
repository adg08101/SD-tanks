import pymysql
import pandas as panda

HOST = ''
USER = ''
PASSWORD = ''
DB = ''
PORT = 0
CONNECTION = ''
NEW_CONNECTION = ''


def main_proc(p_host=HOST, p_user=USER, p_password=PASSWORD, p_db=DB, p_port=PORT, p_connection=CONNECTION):
    print("Connecting...")
    if not p_connection:
        connection = pymysql.connect(host=p_host,
                                     user=p_user,
                                     password=p_password,
                                     database=p_db,
                                     port=int(p_port),
                                     cursorclass=pymysql.cursors.DictCursor)
        global HOST, USER, PASSWORD, DB, PORT, CONNECTION, NEW_CONNECTION
        HOST = p_host
        USER = p_user
        PASSWORD = p_password
        DB = p_db
        PORT = p_port
        CONNECTION = connection
    else:
        connection = CONNECTION

    print("Done... please wait for data retrieval...")

    with connection.cursor() as cursor:
        sql = "SELECT org_name, sd_db_source FROM " + p_db + ".orgs ORDER BY org_name ASC;"
        cursor.execute(sql)
        organizations = cursor.fetchall()

        organizations_to_delete = list()

        for organization in range(len(organizations)):
            sql = "SELECT ACCT_ID FROM " + organizations[organization]['sd_db_source'] + ".tanks AS t WHERE " \
                  "t.TANK_ID IS NOT NULL LIMIT 1; "

            try:
                cursor.execute(sql)
                route = cursor.fetchall()
                if not route:
                    organizations_to_delete.append(organizations[organization]['org_name'])
            except pymysql.err.ProgrammingError:
                organizations_to_delete.append(organizations[organization]['org_name'])

        cursor.close()

    for organization in range(len(organizations)):
        if organizations[organization]['org_name'] in organizations_to_delete:
            organizations[organization]['show'] = 'false'
        else:
            organizations[organization]['show'] = 'true'

    organizations_temp = list()

    for organization in range(len(organizations)):
        if organizations[organization]['show'] == 'true':
            organizations_temp.append(organizations[organization])

    print("-----------------------------")

    for organization in range(len(organizations_temp)):
        print('NUM:', organization, '---- ORG:', organizations_temp[organization]['org_name'])
        print("-----------------------------")

    org_num = get_input("Select organization", 0, len(organizations_temp) - 1)

    org_db = organizations_temp[org_num]['sd_db_source']

    print("Selected Org =", organizations_temp[org_num]['org_name'])
    print("\n")

    if not p_connection:
        new_connection = pymysql.connect(host=p_host,
                                         user=p_user,
                                         password=p_password,
                                         database=org_db,
                                         port=int(p_port),
                                         cursorclass=pymysql.cursors.DictCursor)
        NEW_CONNECTION = new_connection

    proc(connection, NEW_CONNECTION, org_db)


def enable_disable_tank(new_connection, org_db, tank_id):
    sql = "SELECT IS_ACTIVE " \
          "FROM " + org_db + ".tanks AS t " \
                             "WHERE t.TANK_ID = \'%s\' " \
          % tank_id

    tank_status = panda.read_sql(sql, new_connection)

    if tank_status['IS_ACTIVE'][0] == b'\x01':
        sql = "UPDATE " + org_db + ".tanks " \
                "SET IS_ACTIVE = 0 " \
                "WHERE TANK_ID = \'%s\' " \
                % tank_id

        with new_connection.cursor() as cursor:
            cursor.execute(sql)
            new_connection.commit()
            operation = 'disabled'
    else:
        sql = "UPDATE " + org_db + ".tanks " \
                "SET IS_ACTIVE = 1 " \
                "WHERE TANK_ID = \'%s\' " \
                % tank_id

        with new_connection.cursor() as cursor:
            cursor.execute(sql)
            new_connection.commit()
            operation = 'enabled'

    print("Tank", operation, "successfully")


def set_tank_capacity(new_connection, org_db, tank_id):
    capacity = get_input("Input tank new capacity", 0, 1000000)

    sql = "UPDATE " + org_db + ".tanks " \
                               "SET SIZE = \'%s\' " \
                               "WHERE TANK_ID = \'%s\' " \
          % (capacity, tank_id)

    with new_connection.cursor() as cursor:
        cursor.execute(sql)
        new_connection.commit()

    print("Tank capacity adjusted successfully")


def proc(connection, new_connection, org_db):
    sql = "SELECT b.BRANCH_ID, b.BRANCH_NAME FROM " + org_db + ".branch AS b " \
          "INNER JOIN " + org_db + ".account AS a ON a.BRANCH_ID = b.BRANCH_ID " \
          "INNER JOIN " + org_db + ".tanks AS t ON a.ACCT_ID = t.ACCT_ID " \
          "GROUP BY b.BRANCH_NAME "

    branches = panda.read_sql(sql, new_connection)

    print(branches['BRANCH_NAME'])

    print("\n")
    branch = get_input("Select Branch", 0, len(branches) - 1)

    print("Selected branch:")
    print("\n")
    print(branches.iloc[branch])

    account = None

    while True:
        accounts = get_partial_account(new_connection, org_db, branches, branch)

        if int(accounts.shape[0]) > 10:
            print("Too many results, be more precise on account ID")
        elif int(accounts.shape[0]) == 0:
            print("No results, be more precise on account ID")
        elif int(accounts.shape[0]) == 1:
            account = 0
            break
        else:
            break

    print(accounts[['CST_ID_ACCOUNT_LOCATION_ID', 'CST_NAME']])

    if account != 0:
        account = get_input("Select Account", 0, len(accounts) - 1)

    account_id = accounts['ID'].iloc[account]

    print("Selected account:")
    print("\n")
    print(accounts.iloc[account])

    sql = "SELECT TANK_ID, SIZE, SERIAL_NUMBER, IS_ACTIVE, SIZE * CAST(IS_ACTIVE AS UNSIGNED) AS ACTUAL_SIZE " \
          "FROM " + org_db + ".tanks AS t " \
          "WHERE t.ACCT_ID = \'%s\' " \
          % account_id

    tanks = panda.read_sql(sql, new_connection)

    print("\n")
    print("Tank" + "s:" if int(tanks.shape[0]) > 1 else ":")
    print(tanks[['SIZE', 'SERIAL_NUMBER', 'IS_ACTIVE']])
    print("Total available capacity:", tanks['ACTUAL_SIZE'].sum())
    print("Total capacity:", tanks['SIZE'].sum())

    if int(tanks.shape[0]) == 1:
        tank = 0
    else:
        tank = get_input("Select Tank", 0, len(tanks) - 1)

    tank_id = tanks['TANK_ID'].iloc[tank]

    print("\n")
    print("Selected tank:")
    print(tanks.iloc[tank])

    operation = get_input("Select operation: (0) Disable/Enable Tank (1) Set Tank Capacity (2) Both", 0, 2)

    if operation == 0:
        enable_disable_tank(new_connection, org_db, tank_id)
    elif operation == 1:
        set_tank_capacity(new_connection, org_db, tank_id)
    else:
        enable_disable_tank(new_connection, org_db, tank_id)
        set_tank_capacity(new_connection, org_db, tank_id)

    repeat = get_input("Repeat process? (0) No / (1) Yes", 0, 1)
    if repeat == 1:
        main_proc(p_connection=True)
    else:
        connection.close()
        new_connection.close()
        exit(0)


def get_partial_account(new_connection, org_db, branches, branch):
    print("\n")
    partial_account = input("Enter Account ID: ")

    sql = "SELECT a.ACCT_ID AS ID, c.CST_ID, a.ACCOUNT_LOCATION_ID, CONCAT(c.CST_ID, a.ACCOUNT_LOCATION_ID) AS " \
          "CST_ID_ACCOUNT_LOCATION_ID, c.CST_NAME " \
          "FROM " + org_db + ".account AS a " \
          "INNER JOIN " + org_db + ".branch AS b ON b.BRANCH_ID = a.BRANCH_ID " \
          "INNER JOIN " + org_db + ".customer AS c ON a.CUSTOMER_ID = c.CUSTOMER_ID " \
          "INNER JOIN " + org_db + ".tanks AS t ON t.ACCT_ID = a.ACCT_ID " \
          "WHERE b.BRANCH_ID = \'%s\' AND concat(c.CST_ID, a.ACCOUNT_LOCATION_ID) LIKE \'%s\'" \
          "GROUP BY a.ACCT_ID, c.CST_ID, a.ACCOUNT_LOCATION_ID, CONCAT(c.CST_ID, a.ACCOUNT_LOCATION_ID), c.CST_NAME" \
          % (branches['BRANCH_ID'].iat[branch], "%" + partial_account + "%")

    return panda.read_sql(sql, new_connection)


def get_input(text, min_value, max_value):
    while True:
        value = input(text + ": ")

        try:
            if int(value) < min_value or int(value) > max_value or not value.isdigit():
                raise ValueError("value_error")
            else:
                return int(value)
                break
        except ValueError:
            print("Input value error")


if __name__ == '__main__':
    while True:
        host = input("HOST ('smartdrops.gsoftinnovation.net'): ")
        db = input("DB ('smartconnect'): ")
        port = input("PORT (3306):")
        user = input("USER ('root'):")
        password = input("PASS: ")

        if host == '':
            host = 'smartdrops.gsoftinnovation.net'

        if db == '':
            db = 'smartconnect'

        if port == '':
            port = 3306

        if user == '':
            user = 'root'

        try:
            main_proc(host,
                      user,
                      password,
                      db,
                      port,
                      p_connection=False)
            break
        except (pymysql.err.OperationalError, ValueError) as err:
            print("Connection error, could not connect to DB")
            print(err)
