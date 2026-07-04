use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};

pub(super) fn jst_today() -> NaiveDate {
    jst_date(Utc::now())
}

pub(super) fn jst_date(value: DateTime<Utc>) -> NaiveDate {
    let jst = jst_offset();
    value.with_timezone(&jst).date_naive()
}

pub(super) fn jst_datetime(date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let jst = jst_offset();
    jst.from_local_datetime(&date.and_time(time))
        .single()
        .expect("valid JST datetime")
        .with_timezone(&Utc)
}

fn jst_offset() -> FixedOffset {
    FixedOffset::east_opt(9 * 60 * 60).expect("valid JST offset")
}
