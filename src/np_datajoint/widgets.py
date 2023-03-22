import pathlib

import IPython.display

import np_datajoint.classes as classes
import np_datajoint.config as config
from np_datajoint.utils import *


def session_upload_from_acq_widget(cls=classes.DataJointSession) -> ipw.AppLayout:

    sessions = []
    if config.RUNNING_ON_ACQ:
        folders = get_raw_ephys_subfolders(pathlib.Path("A:")) + get_raw_ephys_subfolders(
            pathlib.Path("B:")
        )
        sessions = [
            cls.get_session_folder(folder) 
            for folder in folders 
            if cls.get_session_folder(folder) and is_new_ephys_folder(folder)
        ]
        for session in sessions:
            if sessions.count(session) < 2:
                sessions.remove(session) # folders that aren't split across A:/B:
    
    if not sessions:
        for f in filter(cls.get_session_folder, config.NPEXP_PATH.iterdir()):
            if (
                f.name == cls.get_session_folder(f.name) # excl dir names that have '- copy' appeneded
                and datetime.datetime.strptime(f.name.split('_')[-1], '%Y%m%d') > (datetime.datetime.now() - datetime.timedelta(days=14))
                and not is_hab(f)
            ):
                sessions.append(f.name)

    sessions = sorted(list(set(sessions)))
    probes_available_to_upload = 'ABCDEF'

    out = ipw.Output(layout={"border": "1px solid black"})

    session_dropdown = ipw.Dropdown(
        options=sessions,
        value=None,
        description="session",
        disabled=False,
    )

    upload_button = ipw.ToggleButton(
        description="Upload",
        disabled=True,
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Upload raw data to DataJoint",
        icon="cloud-upload",  # (FontAwesome names without the `fa-` prefix)
    )

    progress_button = ipw.ToggleButton(
        description="Check sorting", 
        disabled=True,
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Check sorting progress on DataJoint",
        icon="hourglass-half",  # (FontAwesome names without the `fa-` prefix)
    )

    surface_image = ipw.Image()
    surface_image.layout.height = '300px'
    surface_image.layout.visibility = 'hidden'
    
    def update_surface_image():
        for img in (config.NPEXP_PATH / session_dropdown.value).glob('*surface-image*'):
            if any(inserted_img in img.name for inserted_img in ('image4', 'image5')):
                break
        else:
            surface_image.layout.visibility = 'hidden'
            return
        with img.open('rb') as f:
            surface_image.value = f.read()
        surface_image.layout.visibility = 'visible'
            
    def handle_dropdown_change(change):
        if cls.get_session_folder(change.new) is not None:
            upload_button.disabled = False
            upload_button.button_style = "warning"
            progress_button.disabled = False
            progress_button.button_style = "info"
            update_surface_image()
    session_dropdown.observe(handle_dropdown_change, names="value")

    def handle_upload_change(change):
        upload_button.disabled = True
        upload_button.button_style = "warning"
        with out:
            logger.info(f"Uploading probes: {probes_from_grid()}")
        session = classes.DataJointSession(session_dropdown.value)
        session.upload(probes=probes_from_grid())

    upload_button.observe(handle_upload_change, names="value")

    def handle_progress_change(change):
        with out:
            logger.info("Fetching summary from DataJoint...")
        progress_button.button_style = ""
        progress_button.disabled = True
        session = classes.DataJointSession(session_dropdown.value)
        try:
            with out:
                IPython.display.display(session.sorting_summary())
        except dj.DataJointError:
            logger.info(
                f"No entry found in DataJoint for session {session_dropdown.value}"
            )

    progress_button.observe(handle_progress_change, names="value")

    buttons = ipw.HBox([upload_button, progress_button])

    probe_select_grid = ipw.GridspecLayout(6, 1, grid_gap="0px")
    for idx, probe_letter in enumerate(probes_available_to_upload):
        probe_select_grid[idx, 0] = ipw.Checkbox(
            value=True,
            description=f"probe{probe_letter}",
            disabled=False,
            indent=True,
        )
    probe_select_and_image = ipw.HBox([surface_image, probe_select_grid])
    def probes_from_grid() -> str:
        probe_letters = ""
        for idx in range(6):
            if probe_select_grid[idx, 0].value == True:
                probe_letters += chr(ord("A") + idx)
        return probe_letters

    app = ipw.TwoByTwoLayout(
        top_right=probe_select_and_image,
        bottom_right=out,
        bottom_left=buttons,
        top_left=session_dropdown,
        width="100%",
        justify_items="center",
        align_items="center",
    )
    return IPython.display.display(app)


def database_diagram() -> IPython.display.SVG:
    diagram = (
        dj.Diagram(config.DJ_SUBJECT.Subject)
        + dj.Diagram(config.DJ_SESSION.Session)
        + dj.Diagram(config.DJ_PROBE)
        + dj.Diagram(config.DJ_EPHYS)
    )
    return diagram.make_svg()
