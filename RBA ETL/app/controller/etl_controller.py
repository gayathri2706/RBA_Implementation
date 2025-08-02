from flask import Blueprint, request, jsonify

from ..security.authorization import key_auth
#from ..services.ui_service import load_measured_java_to_ui_names

from ..services.data_service import get_consumption_bookings, get_additive_raw_data, get_smc_data, run_etl, run_rba_etl

from datetime import datetime

# Create the blue-print
test = Blueprint("test", __name__)


@test.route("test_data", methods=["GET"])
# @intercept_errors
@key_auth
def fetch_sand_params():

    response = dict()
    response["status"] = 200
    response["message"] = "success"
    return jsonify(response)


@test.route("additive_etl_data", methods=["POST"])
# @intercept_errors
@key_auth
def process_additive_etl():
    line_id = int(request.form['foundry_line'])
    date = datetime.strptime(request.form['date'], '%d-%b-%Y')
    customer_id = int(request.form['customer'])

    #get_consumption_bookings(line_id, date, date)
    #get_additive_raw_data(line_id)
    #get_smc_data(line_id, date, date)
    response = run_etl(customer_id,line_id, date, date)


    return jsonify(response)