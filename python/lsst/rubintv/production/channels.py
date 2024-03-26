CHANNELS = ["summit_imexam",
            "summit_specexam",
            "auxtel_mount_torques",
            "auxtel_monitor",
            "all_sky_current",
            "all_sky_movies",
            "auxtel_metadata",
            "auxtel_metadata_creator",
            "auxtel_movies",
            "auxtel_isr_runner",
            "auxtel_night_reports",
            "startracker_night_reports",
            "startracker_raw",
            "startracker_wide_raw",
            "startracker_fast_raw",
            "startracker_analysis",
            "startracker_wide_analysis",
            "startracker_fast_analysis",
            "startracker_metadata",
            "ts8_noise_map",
            "ts8_focal_plane_mosaic",
            "ts8_metadata",
            "comcam_noise_map",
            "comcam_focal_plane_mosaic",
            "comcam_metadata",
            "comcam_sim_noise_map",
            "comcam_sim_focal_plane_mosaic",
            "comcam_sim_metadata",
            "slac_lsstcam_noise_map",
            "slac_lsstcam_focal_plane_mosaic",
            "slac_lsstcam_metadata",
            "tma_mount_motion_profile",
            "tma_m1m3_hardpoint_profile",
            "tma_metadata",
            ]

PREFIXES = {chan: chan.replace('_', '-') for chan in CHANNELS}

KNOWN_INSTRUMENTS = (
    "auxtel",
    "startracker",
    "startracker_wide",
    "startracker_fast",
    "allsky",
    "comcam",
    "comcam_sim",
    "slac_lsstcam",
    "slac_ts8",
    "fake_auxtel",
    "tma",
)


def getCameraAndPlotName(channel):
    """Return the camera name for a given channel name.
    """

    channelLookup = {
        "auxtel_monitor": ("auxtel", "monitor"),
        "summit_imexam": ("auxtel", "imexam"),
        "summit_specexam": ("auxtel", "specexam"),
        "auxtel_mount_torques": ("auxtel", "mount"),
        "auxtel_movies": ("auxtel", "movies"),
        "startracker_raw": ("startracker", "raw"),
        "startracker_analysis": ("startracker", "analysis"),
        "startracker_wide_raw": ("startracker_wide", "raw"),
        "startracker_wide_analysis": ("startracker_wide", "analysis"),
        "startracker_fast_raw": ("startracker_fast", "raw"),
        "startracker_fast_analysis": ("startracker_fast", "analysis"),
        "all_sky_movies": ("allsky", "movies"),
        "all_sky_current": ("allsky", "stills"),
        "comcam_focal_plane_mosaic": ("comcam", "focal_plane_mosaic"),
        "comcam_noise_map": ("comcam", "noise_map"),
        "slac_lsstcam_focal_plane_mosaic": ("slac_lsstcam", "focal_plane_mosaic"),
        "slac_lsstcam_noise_map": ("slac_lsstcam", "noise_map"),
        "slac_lsstcam_ccob_analysis": ("slac_lsstcam", "ccob_analysis"),
        "ts8_focal_plane_mosaic": ("slac_ts8", "focal_plane_mosaic"),
        "ts8_noise_map": ("slac_ts8", "noise_map"),
        "fake_auxtel_monitor": ("fake_auxtel", "monitor"),
        "fake_summit_imexam": ("fake_auxtel", "imexam"),
        "fake_summit_specexam": ("fake_auxtel", "specexam"),
        "fake_auxtel_mount_torques": ("fake_auxtel", "mount"),
        "fake_auxtel_movies": ("fake_auxtel", "movies"),
        "tma_mount_motion_profile": ("tma", "mount"),
        "tma_m1m3_hardpoint_profile": ("tma", "m1m3_hardpoint"),
        "auxtel_metadata": ('auxtel', 'metadata'),
        "auxtel_night_reports": ("auxtel", "night_reports"),
        "startracker_night_reports": ("startracker", "night_reports"),
        "startracker_metadata": ('startracker_narrow', 'metadata'),
        "ts8_metadata": ('slac_ts8', 'metadata'),
        "comcam_metadata": ('comcam', 'metadata'),
        "comcam_sim_metadata": ('comcam_sim', 'metadata'),
        "slac_lsstcam_metadata": ('slac_lsstcam', 'metadata'),
        "tma_metadata": ('tma', 'metadata'),

        # channels which should never upload, so return None to make things
        # raise
        "auxtel_metadata_creator": None,
        "auxtel_isr_runner": ("auxtel", "isr_runner"),
    }
    return channelLookup[channel]
