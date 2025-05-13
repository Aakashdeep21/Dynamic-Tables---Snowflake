# Dynamic-Tables---Snowflake

```sql
create database movies;
```

```sql
use movies;
```
```sql
create or replace table raw_movie_bookings( 
        booking_id string,
        customer_id string,
        movie_id string,
        booking_date timestamp,
        status string,
        ticket_count int,
        ticket_price number(10,2)
);
```
<p>Create a Stream on top of the raw table.</p>

```sql
create or replace stream movie_bookings_stream
on table raw_movie_booking;
```

<p>This is how your Snowflake should look like</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/9dee1e94-bdc6-4b66-ab93-c90199bece40" alt="image" />
</p>

## A Closer Look at Metadata in Snowflake Streams

<p data-start="150" data-end="426">When you create a <strong data-start="168" data-end="178">stream</strong> on a table in Snowflake, it automatically tracks changes to the underlying table and exposes them through <strong data-start="285" data-end="305">metadata columns</strong>. These columns help identify the nature of each change and are essential for implementing <strong data-start="396" data-end="425">Change Data Capture (CDC)</strong>.</p>
<p data-start="428" data-end="487">To view these metadata columns, you can run a simple query:</p>

```sql
SELECT * FROM <your_stream_name>;
```
<p data-start="535" data-end="628">This will return both the data from the source table and additional metadata columns such as:</p>
<ul data-start="630" data-end="838">
<li data-start="630" data-end="707">
<p data-start="632" data-end="707"><code data-start="632" data-end="649">METADATA$ACTION</code> &ndash; Indicates the type of change (<code data-start="682" data-end="690">INSERT</code>, <code data-start="692" data-end="700">DELETE</code>, etc.)</p>
</li>
<li data-start="708" data-end="771">
<p data-start="710" data-end="771"><code data-start="710" data-end="729">METADATA$ISUPDATE</code> &ndash; Boolean flag that is <code data-start="753" data-end="759">TRUE</code> for updates</p>
</li>
<li data-start="772" data-end="838">
<p data-start="774" data-end="838"><code data-start="774" data-end="791">METADATA$ROW_ID</code> &ndash; A unique row identifier for tracking changes</p>
</li>
</ul>


<p align="center">
  <img src="https://github.com/user-attachments/assets/b64b7f84-1f5f-4369-81f0-96cfa9f47c5e" alt="image" />
</p>


## Bronze Table
<p><strong data-start="68" data-end="142">Create a companion table designed to support Change Data Capture (CDC)</strong> by including additional metadata columns such as <code>change_type</code>, <code>is_update</code>, and <code>change_timestamp</code>. This structure enables tracking of data changes over time, providing a clear audit trail for insertions, updates, and deletions.</p>

```sql
create or replace table movie_booking_cdc_events(
        booking_id string,
        customer_id string,
        movie_id string,
        booking_date timestamp,
        status string,
        ticket_count int,
        ticket_price number(10,2),
        change_type string,
        is_update boolean,
        change_timestamp timestamp
);
```


