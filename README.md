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

## Bronze Table
