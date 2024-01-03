import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("We need to bake some cookies! :cookie:")
st.write(
    """
    This app shows how much cookies we expect we 
    need to get ready for the new batch of trial customers.
    """
)

cookie_types = {
    "Läckerli": "PREDICTIONS_LAECKERLI_BOXES",
    "Mandelbärli": "PREDICTIONS_MANDELBAERLI_BOXES",
    "Chocolate Brownies": "PREDICTIONS_CHOCOLATE_BROWNIES_BOXES",
    "Willisauer Ringli": "PREDICTIONS_WILLISAUER_RINGLI_BOXES"
}

sum_predictions = {}

for cookie, table_name in cookie_types.items():
    predictions = session.table(table_name)
    data = predictions.to_pandas()
    cookie_name = cookie.replace(" ", "_")
    sum_predictions[cookie] = round(data[f'"OUTPUT_{cookie_name}_boxes"'].sum())

st.subheader("Cookie type expectations")
st.write(f"""
            According to our best models we expect to need:
            
                {sum_predictions["Läckerli"]} Läckerli boxes
                {sum_predictions["Mandelbärli"]} Mandelbärli boxes
                {sum_predictions["Chocolate Brownies"]} Chocolate Brownies boxes
                {sum_predictions["Willisauer Ringli"]} Willisauer Ringli boxes

            Better get baking!
                
        """)
