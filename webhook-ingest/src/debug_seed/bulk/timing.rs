#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::debug_seed) struct ApiTimingProfile {
    pub(in crate::debug_seed) api: &'static str,
    pub(in crate::debug_seed) publish_lead_base_days: i64,
    pub(in crate::debug_seed) draft_to_publish_base_days: i64,
}

impl ApiTimingProfile {
    pub(in crate::debug_seed) fn publish_lead_days(self, index: u32) -> i64 {
        self.publish_lead_base_days + i64::from(index % 5)
    }

    pub(in crate::debug_seed) fn draft_to_publish_days(self, index: u32) -> i64 {
        self.draft_to_publish_base_days + i64::from(index % 6)
    }
}

const API_TIMING_PROFILES: &[ApiTimingProfile] = &[
    ApiTimingProfile {
        api: "blogs",
        publish_lead_base_days: 1,
        draft_to_publish_base_days: 4,
    },
    ApiTimingProfile {
        api: "authors",
        publish_lead_base_days: 2,
        draft_to_publish_base_days: 2,
    },
    ApiTimingProfile {
        api: "news",
        publish_lead_base_days: 4,
        draft_to_publish_base_days: 10,
    },
    ApiTimingProfile {
        api: "categories",
        publish_lead_base_days: 3,
        draft_to_publish_base_days: 6,
    },
    ApiTimingProfile {
        api: "pages",
        publish_lead_base_days: 8,
        draft_to_publish_base_days: 18,
    },
    ApiTimingProfile {
        api: "advertisements",
        publish_lead_base_days: 5,
        draft_to_publish_base_days: 7,
    },
    ApiTimingProfile {
        api: "tags",
        publish_lead_base_days: 1,
        draft_to_publish_base_days: 1,
    },
    ApiTimingProfile {
        api: "labels",
        publish_lead_base_days: 1,
        draft_to_publish_base_days: 1,
    },
    ApiTimingProfile {
        api: "papers",
        publish_lead_base_days: 14,
        draft_to_publish_base_days: 24,
    },
    ApiTimingProfile {
        api: "cards",
        publish_lead_base_days: 4,
        draft_to_publish_base_days: 5,
    },
];

const DEFAULT_API_TIMING_PROFILE: ApiTimingProfile = ApiTimingProfile {
    api: "*",
    publish_lead_base_days: 5,
    draft_to_publish_base_days: 8,
};

pub(in crate::debug_seed) fn api_timing_profile(api: &str) -> ApiTimingProfile {
    API_TIMING_PROFILES
        .iter()
        .copied()
        .find(|profile| profile.api == api)
        .unwrap_or(DEFAULT_API_TIMING_PROFILE)
}

pub(super) fn api_publish_lead_days(api: &str, index: u32) -> i64 {
    api_timing_profile(api).publish_lead_days(index)
}

pub(super) fn api_draft_to_publish_days(api: &str, index: u32) -> i64 {
    api_timing_profile(api).draft_to_publish_days(index)
}
