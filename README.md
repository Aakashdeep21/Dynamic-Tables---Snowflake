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

##  Loading Stream Data into the Bronze Layer Table

<p data-start="129" data-end="290">Now that the stream is capturing change events, the next step is to <strong data-start="197" data-end="246">ingest this data into your Bronze layer table</strong> &mdash; in this case, <code data-start="263" data-end="289">movie_booking_cdc_events</code>.</p>
<p data-start="292" data-end="397">You can do this by selecting data from the stream and inserting it into the target Delta/Snowflake table:</p>

```sql
insert into movie_booking_cdc_events
select * from <your_stream_name>;
```
<p>To automate the ingestion of stream data into your Bronze layer, you'll want to wrap your <code data-start="103" data-end="111">INSERT</code> logic inside a <strong data-start="127" data-end="145">Snowflake Task</strong>.</p>
<p>To ensure continuous ingestion of change events into your Bronze layer (<code data-start="366" data-end="392">movie_booking_cdc_events</code>), you can define a <strong data-start="412" data-end="430">Snowflake Task</strong> that runs on a schedule or in response to upstream changes.</p>

```sql
create or replace task ingest_stream_to_bronze
warehouse = 'COMPUTE_WH'
schedule = '1 MINUTE'
as
insert into movie_booking_cdc_events
select 
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    METADATA$ACTION as change_type,
    METADATA$ISUPDATE as is_update,
    CURRENT_TIMESTAMP() as change_timestamp
from movie_bookings_stream;
```

<p>After creating the task, you can enable/disable it:</p>

```sql
alter task ingest_stream_to_bronze suspend;
alter task ingest_stream_to_bronze resume;
```

## Building the Silver Layer Using Dynamic Tables

<p data-start="153" data-end="350">In this architecture, the <strong data-start="179" data-end="236">Silver layer tables are implemented as Dynamic Tables</strong> that continuously read from their <strong data-start="271" data-end="296">upstream Bronze layer</strong> &mdash; in this case, the <code data-start="317" data-end="343">movie_booking_cdc_events</code> table.</p>
<p data-start="352" data-end="371">Each dynamic table:</p>
<ul data-start="372" data-end="616">
<li data-start="372" data-end="464">
<p data-start="374" data-end="464">Filters the change data to include only relevant CDC events, such as <code data-start="443" data-end="451">INSERT</code> or <code data-start="455" data-end="463">DELETE</code>.</p>
</li>
<li data-start="465" data-end="551">
<p data-start="467" data-end="551">Applies the necessary <strong data-start="489" data-end="508">transformations</strong> to clean, enrich, or standardize the data.</p>
</li>
<li data-start="552" data-end="616">
<p data-start="554" data-end="616">Automatically updates as new data arrives in the Bronze table.</p>
</li>
</ul>

```sql
create or replace dynamic table filtered_movie_bookings
warehouse = 'COMPUTE_WH'
target_lag = downstream
as
select 
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    max(change_timestamp) as latest_change_timestamp
from movie_booking_cdc_events
where change_type in ('INSERT','DELETE')
group by booking_id,customer_id, movie_id,booking_date,status,ticket_count,ticket_price;
```

## Creating Gold Layer Tables for Aggregated Insights
<p data-start="147" data-end="412">With the Silver layer in place, the next step is to define your <strong data-start="211" data-end="232">Gold layer tables</strong>. These tables serve as <strong data-start="256" data-end="292">curated, business-ready datasets</strong> that deliver value through <strong data-start="320" data-end="360">aggregations, summaries, and metrics</strong> aligned with analytical and reporting requirements.</p>
<p data-start="414" data-end="508">The Gold layer tables read data from the <strong data-start="455" data-end="471">Silver layer</strong> and perform transformations such as:</p>
<ul data-start="510" data-end="691">
<li data-start="510" data-end="557">
<p data-start="512" data-end="557">Aggregating key performance indicators (KPIs)</p>
</li>
<li data-start="558" data-end="623">
<p data-start="560" data-end="623">Calculating trends or summaries (e.g., daily bookings, revenue)</p>
</li>
<li data-start="624" data-end="691">
<p data-start="626" data-end="691">Preparing dimensional views for dashboards or executive reporting</p>
</li>
</ul>

### Example: Booking and Revenue Metrics Table

<p data-start="1404" data-end="1478">In this example, we create a <strong data-start="501" data-end="515">Gold table</strong> that tracks <strong data-start="528" data-end="563">key booking and revenue metrics</strong> for each <strong data-start="573" data-end="582">movie</strong>. The table aggregates the total bookings, tickets sold, revenue, and cancellations for each movie.</p>

```sql
create or replace dynamic table gold_movie_bookings
warehouse = 'COMPUTE_WH'
target_lag = downstream
as
select
    movie_id,
    count(booking_id) as total_bookings,
    sum(case when status = 'COMPLETED' then ticket_count else 0 end) as total_tickets_sold,
    sum(case when status = 'COMPLETED' then ticket_price else 0 end) as total_revenue,
    sum(case when status = 'CANCELLED' then 1 else null end) as total_cancellations,
    current_timestamp() as refresh_timestamp
from filtered_movie_bookings
group by movie_id;
```

<ul>
<li data-start="1402" data-end="1478">
<p data-start="1404" data-end="1478"><code data-start="1404" data-end="1420">total_bookings</code>: The total number of bookings made (<code data-start="1457" data-end="1476">COUNT(booking_id)</code>).</p>
</li>
<li data-start="1481" data-end="1605">
<p data-start="1483" data-end="1605"><code data-start="1483" data-end="1503">total_tickets_sold</code>: The total number of tickets sold, calculated by summing the <code data-start="1565" data-end="1579">ticket_count</code> for <code data-start="1584" data-end="1595">COMPLETED</code> bookings.</p>
</li>
<li data-start="1608" data-end="1712">
<p data-start="1610" data-end="1712"><code data-start="1610" data-end="1625">total_revenue</code>: The total revenue, calculated by summing the <code data-start="1672" data-end="1686">ticket_price</code> for <code data-start="1691" data-end="1702">COMPLETED</code> bookings.</p>
</li>
<li data-start="1715" data-end="1846">
<p data-start="1717" data-end="1846"><code data-start="1717" data-end="1738">total_cancellations</code>: The total number of cancellations, calculated by counting the number of rows where <code data-start="1823" data-end="1845">status = 'CANCELLED'</code></p>
</li>
</ul>
<p data-start="147" data-end="412">.</p>

### Automating Gold Layer Refresh Using Snowflake Tasks

<p data-start="271" data-end="617">To ensure that your <strong data-start="291" data-end="312">Gold layer tables</strong> are always up-to-date and contain fresh data based on specific business requirements, we can leverage <strong data-start="415" data-end="434">Snowflake Tasks</strong>. Tasks allow us to automate the process of refreshing and aggregating data at defined intervals, so the Gold layer continuously reflects the latest information from the Silver layer.</p>
<p data-start="619" data-end="785">By defining a Snowflake Task, we can set a <strong data-start="662" data-end="682">refresh schedule</strong> that runs at regular intervals, ensuring that the <strong data-start="733" data-end="747">Gold table</strong> is updated with the most recent data.</p>

```sql
create or replace task refresh_gold_movie_bookings
warehouse = 'COMPUTE_WH'
schedule = '2 minute'
as
alter dynamic table gold_movie_bookings refresh;
```
<p>After creating the task, you can enable/disable it:</p>

```sql
alter task refresh_gold_movie_bookings suspend;
alter task refresh_gold_movie_bookings resume;
```
#### Verifying Task Status in Snowflake

```sql
select * from table(information_schema.task_history(task_name => 'ingest_stream_to_bronze')) order by scheduled_time;
select * from table(information_schema.task_history(task_name => 'refresh_gold_movie_bookings')) order by scheduled_time;
```

<p>Now that your pipeline is built, it's time to <strong data-start="53" data-end="86">test the end-to-end CDC logic</strong> by inserting a few sample records into your <strong data-start="131" data-end="153">raw (source) table</strong>, which will trigger the downstream flow through Bronze, Silver, and into the Gold layer.</p>

```sql
INSERT INTO raw_movie_booking (booking_id, customer_id, movie_id, booking_date, status, ticket_count, ticket_price)
VALUES
    ('B001', 'C001', 'M001', '2024-12-29 10:00:00', 'BOOKED', 2, 15.00),
    ('B002', 'C002', 'M002', '2024-12-29 10:10:00', 'BOOKED', 1, 12.00),
    ('B003', 'C003', 'M003', '2024-12-29 10:15:00', 'BOOKED', 3, 20.00),
    ('B004', 'C004', 'M004', '2024-12-29 10:20:00', 'BOOKED', 4, 25.00),
    ('B005', 'C005', 'M005', '2024-12-29 10:25:00', 'BOOKED', 1, 10.00);
```

<p>After inserting sample records into the <strong data-start="49" data-end="62">raw table</strong>, it's important to query each layer of your data pipeline&mdash;<strong data-start="121" data-end="128">Raw</strong>, <strong data-start="130" data-end="140">Stream</strong>, <strong data-start="142" data-end="152">Bronze</strong>, <strong data-start="154" data-end="164">Silver</strong>, and <strong data-start="170" data-end="178">Gold</strong>&mdash;to understand how records move through each stage and how transformations take effect.</p>

```sql
select * from raw_movie_booking;
```

![image](https://github.com/user-attachments/assets/7e96ebcb-141c-49dc-a820-782b9a54c888)

```sql
select * from movie_bookings_stream;
```

![image](https://github.com/user-attachments/assets/d28bd128-4872-418a-8035-2da34db88c04)

<p>You might be surprised to find <strong data-start="250" data-end="266">zero records</strong>, even though you just inserted data into the raw table.</p>
<p>Your stream is <strong data-start="358" data-end="379">consumption-based</strong> &mdash; once another object (like your <strong data-start="413" data-end="434">Bronze table load</strong>) reads from the stream, those changes are <strong data-start="477" data-end="499">marked as consumed</strong> and are <strong data-start="508" data-end="529">no longer visible</strong> in subsequent stream queries unless new changes occur.</p>

```sql
select * from movie_booking_cdc_events;
```

![image](https://github.com/user-attachments/assets/cb4c67fe-9aaf-46ca-9cd8-87f737709b85)

```sql
select * from filtered_movie_bookings;
```
![image](https://github.com/user-attachments/assets/48544e4c-e68e-4981-87a5-e723bc04b329)

<p data-start="137" data-end="297">When you query your <strong data-start="157" data-end="179">Silver layer table</strong> (e.g., <code data-start="187" data-end="209">movie_booking_silver</code>), you'll see <strong data-start="223" data-end="251">all the inserted records</strong> that were originally added to your raw table.</p>
<ul data-start="319" data-end="695">
<li data-start="319" data-end="440">
<p data-start="321" data-end="440">When new rows are inserted into the <strong data-start="357" data-end="370">raw table</strong>, the <strong data-start="376" data-end="390">CDC stream</strong> captures them with a <code data-start="412" data-end="425">change_type</code> of <code data-start="429" data-end="439">'INSERT'</code>.</p>
</li>
<li data-start="441" data-end="517">
<p data-start="443" data-end="517">The <strong data-start="447" data-end="463">Bronze layer</strong> persists those changes (including the <code data-start="502" data-end="515">change_type</code>).</p>
</li>
<li data-start="518" data-end="695">
<p data-start="520" data-end="608">The <strong data-start="524" data-end="540">Silver layer</strong> is typically designed to <strong data-start="566" data-end="601">filter and include only records</strong> where:</p>
<ul data-start="611" data-end="695">
<li data-start="611" data-end="640">
<p data-start="613" data-end="640"><code data-start="613" data-end="637">change_type = 'INSERT'</code> or</p>
</li>
<li data-start="643" data-end="695">
<p data-start="645" data-end="695"><code data-start="645" data-end="669">change_type = 'DELETE'</code> <em data-start="670" data-end="695">(if tracking deletions)</em></p>
</li>
</ul>
</li>
</ul>
<p data-start="697" data-end="816">So when you inserted new records, their <code data-start="737" data-end="750">change_type</code> was <code data-start="755" data-end="765">'INSERT'</code>, and they were passed through to the Silver layer.</p>

```sql
select * from gold_movie_bookings;
```

![image](https://github.com/user-attachments/assets/d2322803-70f5-4e09-b2e3-14f58663c532)

<p data-start="197" data-end="306">When the customer <strong data-start="215" data-end="243">initially books a ticket</strong>, the status might be something like <code data-start="280" data-end="290">'BOOKED'</code>. At this point:</p>
<ul data-start="307" data-end="507">
<li data-start="307" data-end="354">
<p data-start="309" data-end="354">The event is <strong data-start="322" data-end="334">inserted</strong> into the raw table.</p>
</li>
<li data-start="355" data-end="423">
<p data-start="357" data-end="423">It flows through the <strong data-start="378" data-end="413">stream &rarr; bronze &rarr; silver &rarr; gold</strong> pipeline.</p>
</li>
<li data-start="355" data-end="423">But in the <strong data-start="437" data-end="463">Gold layer aggregation</strong>, our logic includes: <code></code><code><span class="pl-c1">sum</span>(case when status <span class="pl-k">=</span> <span class="pl-s"><span class="pl-pds">'</span>COMPLETED<span class="pl-pds">'</span></span> then ticket_price else <span class="pl-c1">0</span> end) <span class="pl-k">as</span> total_revenue;</code></li>
</ul>
<p><br />So until that <code data-start="617" data-end="625">status</code> field is updated from <code data-start="648" data-end="658">'BOOKED'</code> to <code data-start="662" data-end="675">'COMPLETED'</code> (or whatever status qualifies for revenue recognition), <strong data-start="732" data-end="759">the revenue won&rsquo;t count</strong>.<br />Same is the case with <code>total_bookings</code>, <code>total_tickets_sold</code> and <code>total_cancellations</code></p>

### Simulate CDC
<p>You can simulate<strong data-start="26" data-end="64"> Change Data Capture (CDC)</strong> by updating records in your raw table, then observe how those changes propagate through your pipeline from <strong data-start="172" data-end="213">raw &rarr; stream &rarr; bronze &rarr; silver &rarr; gold</strong>.</p>

```sql
UPDATE raw_movie_booking
SET status = 'COMPLETED'
WHERE booking_id IN ('B001', 'B003');
```
### Raw Table
```sql
select * from raw_movie_booking;
```

![image](https://github.com/user-attachments/assets/0cae7e11-6d81-4ae4-bcc3-dd8ffaad2514)


### Bronze Table

```sql
select * from movie_booking_cdc_events;
```

![image](https://github.com/user-attachments/assets/a2c4f021-adff-4f5d-806d-054b4561e51b)


### Silver Table
```sql
select * from filtered_movie_bookings;
```

![image](https://github.com/user-attachments/assets/b14dee41-35d0-4fed-8368-b73bb9d7b423)

### Gold Table
```sql
select * from gold_movie_bookings;
```

![image](https://github.com/user-attachments/assets/02be0b49-b851-4b5c-8541-d21a4533db96)


