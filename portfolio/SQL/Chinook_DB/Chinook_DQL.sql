-- Data Query Language

# All artists in the database.
select * from `Artist`;

# All tracks available for each album.
select ar.`Name` as Artist, ab.`Title` as AlbumName, tr.`Name` as TrackName
from `Album` as ab
left join `Artist` as ar on ar.`ArtistId` = ab.`ArtistId`
left join `MusicTrack` as tr on tr.`AlbumId` = ab.`AlbumId`
order by ar.`ArtistId`, ab.`Title`, tr.`Name`;

# All customers who have purchased music in a 'Jazz' genre.
select concat(cr.`FirstName`, ' ', cr.`LastName`) as Customer, count(tr.`TrackId`) as TracksCount
from `Customer` as cr
join `Invoice` as i on i.`CustomerId` = cr.`CustomerId`
join `InvoiceLine` as il on il.`InvoiceId` = i.`InvoiceId`
join `MusicTrack` as tr on tr.`TrackId` = il.`TrackId`
join `Genre` as gr on gr.`GenreId` = tr.`GenreId`
where gr.Name = 'Jazz'
group by cr.`CustomerId`;

# Total number of invoices per customer.
select concat(cr.`FirstName`, ' ', cr.`LastName`) as Customer, count(`InvoiceId`) as InvoicesCount
from `Invoice` as i
join `Customer` as cr on cr.`CustomerId` = i.`CustomerId`
group by i.`CustomerId`
order by InvoicesCount desc;

# Top 5 countries with the highest number of customers.
select `Country`, count(`CustomerId`) as CustomersCount
from `Customer`
group by `Country`
order by CustomersCount desc
limit 5;

# All genres that have more than 100 track
select gr.`Name` as Genre, count(tr.`TrackId`) as TracksCount
from `MusicTrack` as tr
join `Genre`as gr on tr.`GenreId` = gr.`GenreId`
group by gr.`GenreId`
having count(tr.`TrackId`) > 100;

# Sales revenue for each genre.
select gr.`Name` as Genre, sum(il.`UnitPrice` * il.`Quantity`) as SalesRevenue
from `InvoiceLine` il
join `MusicTrack` tr on il.`TrackId` = tr.`TrackId`
join `Genre` gr on tr.`GenreId` = gr.`GenreId`
join `Invoice` i on il.`InvoiceId` = i.`InvoiceId`
group by gr.`GenreId`
order by SalesRevenue desc;