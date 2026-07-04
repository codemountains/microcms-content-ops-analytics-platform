const BULK_ACTIVITY_WEIGHT_TOTAL: u32 = 2_000;
const BULK_WEIGHT_INITIAL_DRAFT: u32 = 400;
const BULK_WEIGHT_SAVE_DRAFT: u32 = 300;
const BULK_WEIGHT_PUBLISH_FROM_DRAFT: u32 = 53;
const BULK_WEIGHT_INITIAL_PUBLISH: u32 = 12;
const BULK_WEIGHT_UPDATE_PUBLISHED: u32 = 377;
const BULK_WEIGHT_ADD_DRAFT_TO_PUBLISHED: u32 = 400;
const BULK_WEIGHT_DISCARD_DRAFT_ON_PUBLISHED: u32 = 100;
const BULK_WEIGHT_UNPUBLISH_TO_DRAFT: u32 = 160;
const BULK_WEIGHT_UNPUBLISH_TO_CLOSED: u32 = 80;
const BULK_WEIGHT_REOPEN_TO_DRAFT: u32 = 20;
const BULK_WEIGHT_REPUBLISH_FROM_CLOSED: u32 = 8;
const BULK_WEIGHT_DELETE_DRAFT: u32 = 20;
const BULK_WEIGHT_DELETE_PUBLISHED: u32 = 60;
const BULK_WEIGHT_DELETE_CLOSED: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::debug_seed) struct ActivityTargets {
    pub(in crate::debug_seed) initial_draft: u32,
    pub(in crate::debug_seed) save_draft: u32,
    pub(in crate::debug_seed) publish_from_draft: u32,
    pub(in crate::debug_seed) initial_publish: u32,
    pub(in crate::debug_seed) update_published: u32,
    pub(in crate::debug_seed) add_draft_to_published: u32,
    pub(in crate::debug_seed) discard_draft_on_published: u32,
    pub(in crate::debug_seed) unpublish_to_draft: u32,
    pub(in crate::debug_seed) unpublish_to_closed: u32,
    pub(in crate::debug_seed) reopen_to_draft: u32,
    pub(in crate::debug_seed) republish_from_closed: u32,
    pub(in crate::debug_seed) delete_draft: u32,
    pub(in crate::debug_seed) delete_published: u32,
    pub(in crate::debug_seed) delete_closed: u32,
}

impl ActivityTargets {
    pub(in crate::debug_seed) fn total(self) -> u32 {
        self.initial_draft
            + self.save_draft
            + self.publish_from_draft
            + self.initial_publish
            + self.update_published
            + self.add_draft_to_published
            + self.discard_draft_on_published
            + self.unpublish_to_draft
            + self.unpublish_to_closed
            + self.reopen_to_draft
            + self.republish_from_closed
            + self.delete_draft
            + self.delete_published
            + self.delete_closed
    }
}

pub(in crate::debug_seed) fn compute_activity_targets(total: u32, _days: u32) -> ActivityTargets {
    let weights = [
        BULK_WEIGHT_INITIAL_DRAFT,
        BULK_WEIGHT_SAVE_DRAFT,
        BULK_WEIGHT_PUBLISH_FROM_DRAFT,
        BULK_WEIGHT_INITIAL_PUBLISH,
        BULK_WEIGHT_UPDATE_PUBLISHED,
        BULK_WEIGHT_ADD_DRAFT_TO_PUBLISHED,
        BULK_WEIGHT_DISCARD_DRAFT_ON_PUBLISHED,
        BULK_WEIGHT_UNPUBLISH_TO_DRAFT,
        BULK_WEIGHT_UNPUBLISH_TO_CLOSED,
        BULK_WEIGHT_REOPEN_TO_DRAFT,
        BULK_WEIGHT_REPUBLISH_FROM_CLOSED,
        BULK_WEIGHT_DELETE_DRAFT,
        BULK_WEIGHT_DELETE_PUBLISHED,
        BULK_WEIGHT_DELETE_CLOSED,
    ];
    let mut counts = [0_u32; 14];
    let mut assigned = 0_u32;
    for (index, weight) in weights.into_iter().enumerate() {
        counts[index] = total * weight / BULK_ACTIVITY_WEIGHT_TOTAL;
        assigned += counts[index];
    }
    counts[13] += total.saturating_sub(assigned);

    ActivityTargets {
        initial_draft: counts[0],
        save_draft: counts[1],
        publish_from_draft: counts[2],
        initial_publish: counts[3],
        update_published: counts[4],
        add_draft_to_published: counts[5],
        discard_draft_on_published: counts[6],
        unpublish_to_draft: counts[7],
        unpublish_to_closed: counts[8],
        reopen_to_draft: counts[9],
        republish_from_closed: counts[10],
        delete_draft: counts[11],
        delete_published: counts[12],
        delete_closed: counts[13],
    }
}
