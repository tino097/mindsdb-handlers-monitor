-- MS SQL Server TPC-H Sample Data Loading
-- Loads sample data for testing purposes

USE TestDB;
GO

-- Insert Regions
INSERT INTO region (r_regionkey, r_name, r_comment) VALUES
(0, 'AFRICA', 'lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to'),
(1, 'AMERICA', 'hs use ironic, even requests. s'),
(2, 'ASIA', 'ges. thinly even pinto beans ca'),
(3, 'EUROPE', 'ly final courts cajole furiously final excuse'),
(4, 'MIDDLE EAST', 'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');
GO

-- Insert Nations
INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES
(0, 'ALGERIA', 0, ' haggle. carefully final deposits detect slyly agai'),
(1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon'),
(2, 'BRAZIL', 1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special'),
(3, 'CANADA', 1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
(4, 'EGYPT', 4, 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'),
(5, 'ETHIOPIA', 0, 'ven packages wake quickly. regu'),
(6, 'FRANCE', 3, 'refully final requests. regular, ironi'),
(7, 'GERMANY', 3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
(8, 'INDIA', 2, 'ss excuses cajole slyly across the packages. deposits print aroun'),
(9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'),
(10, 'IRAN', 4, 'efully alongside of the slyly final dependencies.'),
(11, 'IRAQ', 4, 'nic deposits boost atop the quickly final requests? quickly regula'),
(12, 'JAPAN', 2, 'ously. final, express gifts cajole a'),
(13, 'JORDAN', 4, 'ic deposits are blithely about the carefully regular pa'),
(14, 'KENYA', 0, ' pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t'),
(15, 'MOROCCO', 0, 'rns. blithely bold courts among the closely regular packages use furiously bold platelets?'),
(16, 'MOZAMBIQUE', 0, 's. ironic, unusual asymptotes wake blithely r'),
(17, 'PERU', 1, 'platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun'),
(18, 'CHINA', 2, 'c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos'),
(19, 'ROMANIA', 3, 'ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account'),
(20, 'SAUDI ARABIA', 4, 'ts. silent requests haggle. closely express packages sleep across the blithely'),
(21, 'VIETNAM', 2, 'hely enticingly express accounts. even, final'),
(22, 'RUSSIA', 3, 'requests against the platelets use never according to the quickly regular pint'),
(23, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are. carefull'),
(24, 'UNITED STATES', 1, 'y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be');
GO

-- Insert Sample Customers
INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES
(1, 'Customer#000000001', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'to the even, regular platelets. regular, ironic epitaphs nag e'),
(2, 'Customer#000000002', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'l accounts. blithely ironic theodolites integrate boldly: caref'),
(3, 'Customer#000000003', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', ' deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov'),
(4, 'Customer#000000004', 'XxVSJsLAGtn', 4, '14-128-190-5944', 2866.83, 'MACHINERY', ' requests. final, regular ideas sleep final accou'),
(5, 'Customer#000000005', 'KvpyuHCplrB84WgAiGV6sYpZq7Tj', 3, '13-750-942-6364', 794.47, 'HOUSEHOLD', 'n accounts will have to unwind. foxes cajole accor');
GO

-- Insert Sample Suppliers
INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES
(1, 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', 17, '27-918-335-1736', 5755.94, 'each slyly above the careful'),
(2, 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', 5, '15-679-861-2259', 4032.68, ' slyly bold instructions. idle dependen'),
(3, 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', 1, '11-383-516-1199', 4192.40, 'blithely silent requests after the express dependencies are sl'),
(4, 'Supplier#000000004', 'Bk7ah4CK8SYQTepEmvMkkgMwg', 15, '25-843-787-7479', 4641.08, 'riously even requests above the exp'),
(5, 'Supplier#000000005', 'Gcdm2rJRzl5qlTVzc', 11, '21-151-690-3663', 9169.10, 'elets. regular deposits cajole slyly. furiously final pinto beans wake slyly');
GO

-- Insert Sample Parts
INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) VALUES
(1, 'goldenrod lavender spring chocolate lace', 'Manufacturer#1', 'Brand#13', 'PROMO BURNISHED COPPER', 7, 'JUMBO PKG', 901.00, 'ly. slyly ironi'),
(2, 'blush thistle blue yellow saddle', 'Manufacturer#1', 'Brand#13', 'LARGE BRUSHED BRASS', 1, 'LG CASE', 902.00, 'lar accounts amo'),
(3, 'spring green yellow purple cornsilk', 'Manufacturer#4', 'Brand#42', 'STANDARD POLISHED BRASS', 21, 'WRAP CASE', 903.00, 'egular deposits hag'),
(4, 'cornflower chocolate smoke green pink', 'Manufacturer#3', 'Brand#34', 'SMALL PLATED BRASS', 14, 'MED DRUM', 904.00, 'p furiously r'),
(5, 'forest brown coral puff cream', 'Manufacturer#3', 'Brand#32', 'STANDARD POLISHED TIN', 15, 'SM PKG', 905.00, 'wake carefully');
GO

-- Insert Sample Orders
INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES
(1, 1, 'O', 173665.47, '1996-01-02', '5-LOW', 'Clerk#000000951', 0, 'nstructions sleep furiously among'),
(2, 2, 'O', 46929.18, '1996-12-01', '1-URGENT', 'Clerk#000000880', 0, 'foxes. pending accounts at the pending, silent asymptot'),
(3, 3, 'F', 193846.25, '1993-10-14', '5-LOW', 'Clerk#000000955', 0, 'sly final accounts boost. carefully regular ideas cajole carefully. depos'),
(4, 4, 'O', 32151.78, '1995-10-11', '5-LOW', 'Clerk#000000124', 0, 'sits. slyly regular warthogs cajole. regular, regular theodolites acro'),
(5, 5, 'F', 144659.20, '1994-07-30', '5-LOW', 'Clerk#000000925', 0, 'quickly. bold deposits sleep slyly. packages use slyly');
GO

-- Insert Sample Partsupp
INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) VALUES
(1, 1, 3325, 771.64, 'final theodolites'),
(1, 2, 8076, 993.49, 'ven ideas. quickly even packages print. pending multipliers must have to are fluff'),
(2, 1, 3956, 337.09, 'after the fluffily ironic deposits'),
(2, 2, 8895, 378.49, 'ously regular deposits haggle slyly. furiously bold pinto beans haggle carefully. flu'),
(3, 1, 4651, 920.92, 'inal foxes hang. slyly bold packages'),
(3, 2, 4093, 498.13, 'eep carefully. slyly express accounts among the packages nag carefully'),
(4, 1, 1620, 113.97, 'lites. fluffily pending dependencies use slyly across the furiously regular theodolites. blithel'),
(5, 1, 8538, 357.84, 'ily ironic packages wake. ironic deposits a');
GO

-- Insert Sample Lineitems
INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES
(1, 1, 1, 1, 17, 21168.23, 0.04, 0.02, 'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above the'),
(1, 2, 1, 2, 36, 45983.16, 0.09, 0.06, 'N', 'O', '1996-04-12', '1996-02-28', '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'ly final dependencies: slyly bold'),
(2, 3, 1, 1, 38, 44694.46, 0.00, 0.05, 'N', 'O', '1997-01-28', '1997-01-14', '1997-02-02', 'TAKE BACK RETURN', 'RAIL', 'ven requests. deposits breach a'),
(3, 4, 1, 1, 45, 54058.05, 0.06, 0.00, 'R', 'F', '1994-02-02', '1994-01-04', '1994-02-23', 'NONE', 'AIR', 'ongside of the furiously brave acco'),
(3, 5, 1, 2, 49, 65149.06, 0.10, 0.00, 'R', 'F', '1993-11-09', '1993-12-20', '1993-11-24', 'TAKE BACK RETURN', 'RAIL', ' unusual accounts. eve');
GO

PRINT 'Sample data loading completed successfully!';
GO