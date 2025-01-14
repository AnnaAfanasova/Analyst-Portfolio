-- Data Manipulation Language

-- Insert data for suppliers into the Supplier table.

insert into Supplier (SupplierId, SupplierName, ContactName, ContactEmail, Phone, Country)
values 
(1, 'Global Music Supplies', 'John Doe', 'johndoe@globalmusic.com', '+1-800-123-4567', 'USA'),
(2, 'SoundWorks Ltd.', 'Jane Smith', 'janesmith@soundworks.com', '+44-20-7946-0852', 'UK'),
(3, 'Melody Distributors', 'Alice Johnson', 'alice.j@melodydist.com', '+33-1-70-18-31-09', 'France'),
(4, 'Beat Factory Inc.', 'Bob Williams', 'bobw@beatfactory.com', '+61-2-9876-5432', 'Australia'),
(5, 'Harmony Partners', 'Chris Brown', 'chris.brown@harmonypartners.com', '+49-30-2049-7856', 'Germany');

-- Insert data into Genre table: add a new name and assign it to several tracks.
insert into `Genre`(`GenreId`, Name)
values (26, 'Folk');

update `MusicTrack`
set GenreID = (select `GenreId` from `Genre` where Name = 'Folk')
where `TrackId` in (30, 31, 32, 33, 34);

-- Update the billing address for a specific customer.
update `Customer`
set `Address` = 'Musterstraße 45', `City` = 'Berlin'
where `CustomerId` = 2;

-- Delete a track from the database based on its track ID.
delete from `MusicTrack`
where `TrackId` = 32;