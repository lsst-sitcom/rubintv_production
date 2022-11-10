CHANNELS = ["summit_imexam",
            "summit_specexam",
            "auxtel_mount_torques",
            "auxtel_monitor",
            "all_sky_current",
            "all_sky_movies",
            "auxtel_metadata",
            "auxtel_movies",
            "auxtel_isr_runner",
            "startracker",
            "startracker_analysis",
            "startracker_wide",
            "startracker_wide_analysis",
            "startracker_metadata",
            ]

PREFIXES = {chan: chan.replace('_', '-') for chan in CHANNELS}
