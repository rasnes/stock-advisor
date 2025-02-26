import hmac
import streamlit as st
from os import environ
from dotenv import load_dotenv

load_dotenv()

def check_password() -> bool:
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Password incorrect")
    return False

# Check if authentication is required based on environment
REQUIRE_AUTH = st.secrets.get("REQUIRE_AUTH", True)
if environ.get("DEVELOPMENT") == "local":
    REQUIRE_AUTH = False

if REQUIRE_AUTH and not check_password():
    st.stop()

st.logo("dashboard/artifacts/pitchit_hq.png", size="large")

dashboards = dict(
    main=st.Page(
        "dashboards/main.py",
        title="Dashboard",
        icon=":material/dashboard:",
        default=True,
    ),
    stock_picker=st.Page(
        "dashboards/stock_picker.py",
        title="Stock Picker",
        icon=":material/dashboard:",
    ),
    docs=st.Page(
        "dashboards/docs.py",
        title="Documentation",
        icon=":material/dashboard:",
    ),
)

notebooks = dict(
    catboost_model_12m=st.Page(
        "notebooks/catboost_model_12m.py",
        icon=":material/insert_drive_file:",
    ),
    catboost_model_24m=st.Page(
        "notebooks/catboost_model_24m.py",
        icon=":material/insert_drive_file:",
    ),
    catboost_model_36m=st.Page(
        "notebooks/catboost_model_36m.py",
        icon=":material/insert_drive_file:",
    ),
)

pg = st.navigation(
    {
        "Dashboards": [*dashboards.values()],
        "Notebooks": [*notebooks.values()],
    }
)

pg.run()
