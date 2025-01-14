-- Data Definition Language

-- Create a table to store supplier data, including ID, supplier name, contact information, and country.
create table Supplier (
    SupplierId int primary key,
    SupplierName varchar(100),
    ContactName varchar(100),
    ContactEmail varchar(100),
    Phone varchar(20),
    Country varchar(50)
);

-- Create a linking table between suppliers and tracks.
create table SupplierTrack (
    SupplierTrackId int primary key,
    SupplierId int,
    TrackId int,
    foreign key (SupplierId) references Supplier(SupplierId),
    foreign key (TrackId) references Track(TrackId)
);

-- Add a Discount column to the Invoice table to store discounts on each invoice.
alter table `Invoice`
add column Discount decimal(5, 2)
default 0.00;

-- Rename the Track table to MusicTrack.
alter table Track rename to MusicTrack;

-- Create an index on the ArtistId column in the Album table to speed up the search for albums by artist.
create index idx_artist_id on Album (ArtistID);

-- Drop the Album table from the database.
drop table if exists Album;