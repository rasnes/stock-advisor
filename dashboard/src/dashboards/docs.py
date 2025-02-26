import streamlit as st

st.title("Documentation")

st.markdown("""
## Usage

### Dashboard

In this dashboard, you can see and compare the historical development of a stocks together with their forecasted developments and uncertainty estimates.

By default it will show the latest predictions available (should be within the last week), but you can select previouse `Trained Date`s in the dropdown list in the middel of the dashboard.

### Stock Picker

In this dashboard you can drill deeper into stocks you want to investigate. In the top table you can select the stock and prediction horizon, or row, of interest that you want to see the SHAP values for and compare with other selections.

SHAP values are, in short, a metric for explaining _why_ predictions get the values they get. The values are ordered by absolute value descending, meaning that most important variables for the predictions are listed on the top. Negative SHAP values means that the specific feature value contributes negatively to the final prediction value (i.e. the feature makes it _less_ likely that the stock will beat the index), and vice versa.

## The deeper dive

For a more thorough walk-through of how the model was developed and the assumptions made, see this post: https://rasmusnes.com/posts/stock-advisor-model/
""")
