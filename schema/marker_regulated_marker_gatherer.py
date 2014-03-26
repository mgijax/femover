#!/usr/local/bin/python
# 
# gathers data for the 'marker_regulated_marker' and 'marker_reg_property'
# tables in the front-end database

import sys
import KeyGenerator
import Gatherer
import logger
import types
import dbAgnostic
import ListSorter

###--- Sample Data ---###

SampleData = \
"""Add	regulates	MGI:4950384	Mir1843b	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-5134;miTG_score=0.98	from wendy's file
Add	regulates	MGI:2676854	Mir190	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-190-5p;miTG_score=0.972	from wendy's file
Add	regulates	MGI:3619323	Mir300	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-300-3p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:2684360	Mir223	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-223-3p;miTG_score=0.961	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.979	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:3718457	Mir190b	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-190b-5p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:3718516	Mir421	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-421-3p;miTG_score=0.99	from wendy's file
Add	regulates	MGI:4834330	Mir374c	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-374c-5p;miTG_score=0.978	from wendy's file
Add	regulates	MGI:2676876	Mir201	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-201-3p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:3619374	Mir369	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-369-3p;miTG_score=0.965	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:3045322	4932411N23Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.97	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1921766	A730049H05Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.955	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1916882	Arhgef12	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:104783	Atxn1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:2179277	Atxn7	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.963	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1914971	Atxn7l3b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.963	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1316660	Cacng2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=1	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:2681120	Chsy1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.976	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1858500	Diap2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1920179	Dip2c	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.988	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1100887	Elavl2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.951	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:107427	Elavl4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=1	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1926116	Fam175b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.96	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1921303	Grip1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.955	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:109442	Itga8	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.967	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:2443439	Itgbl1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.963	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:3029632	Med13	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.976	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:97311	Nfix	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1316652	Prmt2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.958	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:97837	Qk	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.98	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1917200	Rcbtb2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.973	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1201673	Shox2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.991	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1203732	Slc16a2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=1	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1929213	Zbtb20	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.996	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:99948	Zfhx3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:107690	Zfp462	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:3619376	Mir375	RV:0000102	miRNA silences	MGI:1334444	Zfpm2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-375-3p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1917946	3830417A13Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.98	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2683305	A430089I19Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1338038	Aebp2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.981	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2653612	Amigo1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.957	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2444029	Ankrd52	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2144383	Ccdc117	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.989	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1860776	Chdh	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.963	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2451355	Crxos1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1196281	Dscam	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.965	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:102853	Elk4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1347464	Foxg1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.989	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:95610	Gabpa	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.955	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1277216	Gtf2h1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.981	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1917171	Hnrnpa3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.992	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:3045337	Hsf3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:96604	Itga5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:96661	Kcna4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.957	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:102699	Mtcp1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.969	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1932545	Ndst4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2664186	Npas4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.97	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1932115	Papolb	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.992	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1919871	Pdzd11	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.951	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1915148	Pef1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.961	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:1933165	Plagl2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.957	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2138986	Qser1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-3p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2444236	Ubn2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:3617791	Mir378	RV:0000102	miRNA silences	MGI:2444233	Ythdf2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-378-5p;miTG_score=0.962	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918925	0610010F05Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1921298	4632428N05Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.975	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918204	4922502B01Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.993	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443361	6430550D23Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443215	A330008L17Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.993	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1351644	Abcc5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.958	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1913332	Abhd6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1933148	Abtb1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.991	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1890410	Acss2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2147586	AI606181	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.983	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2444031	Alg6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.973	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2151224	Alpk3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.967	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1917904	Ankrd33b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:5000466	Anpep	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1328360	Arid3a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.968	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:104783	Atxn1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.954	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2143205	AU019823	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.992	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1097161	Bak1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1201607	Blzf1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2176433	Bmf	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2388804	Brms1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.96	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1926033	Brpf1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.976	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1890651	Brwd1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.955	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:102522	Cacnb1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.963	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:88329	Cd34	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.988	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914322	Cdc37l1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.974	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2652845	Cdc42bpg	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1889510	Cdc42se1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:105057	Cdh5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.972	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:107433	Cdh9	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.972	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1927237	Cgn	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.975	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1915817	Cgref1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2444898	Chfr	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443370	Chtf8	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.982	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1345966	Coro2a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1913948	Cpsf6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.983	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2679260	Crb2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1353467	Cttnbp2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:90673	D17H6S53E	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914596	Daam1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.982	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2445102	Dhx33	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.985	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914421	Dram2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1915980	Dus1l	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.996	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914367	Dynlt3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.981	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1917110	Eif1ad	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:109342	Eif2d	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1346831	Esrra	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:109336	Etv6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1920475	Fam118a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.973	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2388278	Fam134a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2145900	Fam83h	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1354698	Fbxw4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2442557	Frmd5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2685834	Gal3st2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918935	Galnt14	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2179403	Galnt5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1913581	Galntl6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.969	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:95676	Gcnt1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1921355	Gga2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:95801	Grk4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.957	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1860138	Gtpbp2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2678023	Homez	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.954	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:3588250	I830134H01Rik	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.992	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:3027003	Ino80d	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.991	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1924315	Ints7	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.96	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1096873	Irf4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1098804	Kcns3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:107953	Klc2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.978	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1354948	Klf13	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1933395	Lactb	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1925139	Lbh	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.995	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1095413	Lfng	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:3584032	Lin28b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:106096	Loxl1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.989	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1917780	Lrfn2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.975	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2685551	Lrrc10b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.989	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:96904	M6pr	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2684954	Man1b1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.957	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1346879	Map3k10	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.993	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1346880	Map3k11	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1346883	Map4k2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1353438	Mapk12	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.974	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1098644	Mfhas1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443548	Mfsd9	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:107376	Msi1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.97	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1100535	Myt1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1926081	Ncln	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.96	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1861721	Necab3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.995	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:97305	Neu1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:105108	Nin	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.956	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2444671	Nipal4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.982	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1921341	Npl	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:104750	Nrcam	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.979	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1202070	Nsg2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1859555	Nup210	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.997	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1328306	Orc2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1923784	Osbpl9	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1855700	Pcdh12	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:107421	Pcsk7	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.959	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2387203	Ppat	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.975	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:99655	Prdm1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.977	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1930121	Prdm15	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.962	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918029	Prdm5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.96	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2444710	Prtg	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.99	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:97805	Ptpn1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:108410	Ptpn18	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.988	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:97852	Rap1a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.973	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1920963	Rbm20	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.971	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1333865	Rfxank	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.968	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1931553	Rhoq	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.966	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:104661	Rora	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.987	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443872	Samd10	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2384298	Sbno1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918395	Scara5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.951	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1915065	Sec14l2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.98	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1929083	Sectm1b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.981	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1929083	Sectm1b	Not specified	IEA	J:88888	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:109252	Sema4c	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.973	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:109244	Sema4d	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1261415	Sgpl1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.981	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2444417	Sh3tc2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1919248	Slc25a35	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.982	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1915093	Slc35a4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914820	Slc39a9	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918956	Slc46a3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2150150	Slc4a10	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2443198	Slitrk6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.999	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1915984	Smek1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1919742	Smg1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.996	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1919541	Sntg2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.996	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:109282	Speg	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.972	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:98329	Sstr3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.988	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1341894	St6galnac4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:106018	St8sia4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.954	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2385331	Stard13	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.994	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1919399	Strada	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.972	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1921376	Syvn1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.996	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:3039562	Taf9b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.989	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1889508	Tbc1d1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:105080	Tgoln1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.995	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1919995	Tmem161b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.975	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2682935	Tmprss13	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.992	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914057	Tmtc2	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.995	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1196377	Tnfaip3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.98	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:104511	Tnfsf4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1916326	Triap1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.967	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1927616	Trps1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.977	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1891307	Ubn1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.998	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1925027	Ulbp1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-3p;miTG_score=0.984	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1918992	Ulk3	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.99	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1100499	Vps4b	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.986	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1888526	Xpo4	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.951	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1352495	Zfp385a	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.97	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2180715	Zfp704	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.979	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:3026685	Zfyve1	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=0.993	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2139317	Zscan29	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1921714	Zswim5	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:1914513	Zswim6	Not specified	IEA	J:12345	wpitman	mature_transcript=mmu-miR-125a-5p;miTG_score=1	from wendy's file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:101757	Cfl1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:102764	Six3	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:107793	Crmp1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:1341840	Six6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:2151153	Boc	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:2447063	Tenm4	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:88357	Cdk4	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:98364	Sox2	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97490	Pax6	RV:0000006	regulates	MGI:98506	Tcf4	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:104717	Meis1	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:108564	Meis2	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:3619436	Mir7-1	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:3619436	Mir7-1	RV:0000102	miRNA silences	MGI:97490	Pax6	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97495	Pbx1	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1201409	Pknox1	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:2445415	Pknox2	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101898	Pou2f1	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:102764	Six3	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:102764	Six3	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:98364	Sox2	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1277163	Vax1	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1346018	Vax2	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1346018	Vax2	RV:0000006	regulates	MGI:97490	Pax6	Not specified	IDA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:2676809	Mir125a	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:2676793	Mirlet7a-1	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:2676793	Mirlet7a-1	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:3619049	Mirlet7a-2	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:3619049	Mirlet7a-2	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:2676797	Mirlet7e	RV:0000102	miRNA silences	MGI:2685973	Trim71	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1261827	Dnmt3a	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:1261827	Dnmt3a	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1261827	Dnmt3a	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1261819	Dnmt3b	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:1261819	Dnmt3b	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:22222	jlewis		Mined from GAF file
Add	regulates	MGI:1261819	Dnmt3b	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:33333	jlewis		Mined from GAF file
Add	regulates	MGI:1100846	Med1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1926212	Med12	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1913976	Nipbl	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:108011	Nobox	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:108011	Nobox	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1352451	Nr2f1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:1352451	Nr2f1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1352452	Nr2f2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1352452	Nr2f2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:1346834	Nr5a2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1346834	Nr5a2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1352459	Nr6a1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:1352459	Nr6a1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:22222	jlewis		Mined from GAF file
Add	regulates	MGI:1352459	Nr6a1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:33333	jlewis		Mined from GAF file
Add	regulates	MGI:1352459	Nr6a1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:44444	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:97856	Rara	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:97856	Rara	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:22222	jlewis		Mined from GAF file
Add	regulates	MGI:97856	Rara	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:33333	jlewis		Mined from GAF file
Add	regulates	MGI:97857	Rarb	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:44444	jlewis		Mined from GAF file
Add	regulates	MGI:97857	Rarb	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:55555	jlewis		Mined from GAF file
Add	regulates	MGI:97857	Rarb	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:66666	jlewis		Mined from GAF file
Add	regulates	MGI:97858	Rarg	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98214	Rxra	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98215	Rxrb	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98215	Rxrb	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98216	Rxrg	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98216	Rxrg	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:1344345	Smc1a	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1339795	Smc3	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98364	Sox2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:98364	Sox2	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:1197010	Sumo1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:1197010	Sumo1	RV:0000006	regulates	MGI:101893	Pou5f1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:1098693	Tet1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:1276125	Utf1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:1354755	Fbxo15	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:1354755	Fbxo15	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:1919200	Nanog	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:2443298	Tet2	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:95518	Fgf4	Not specified	IEA	J:11111	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:95518	Fgf4	Not specified	IEA	J:22222	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:95518	Fgf4	Not specified	IEA	J:33333	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:95518	Fgf4	Not specified	IEA	J:44444	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:95518	Fgf4	Not specified	IEA	J:55555	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:96770	Lef1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98364	Sox2	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98364	Sox2	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98364	Sox2	Not specified	IEA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98389	Spp1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98389	Spp1	Not specified	IDA	J:67890	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98389	Spp1	Not specified	IEA	J:99999	jlewis		Mined from GAF file
Add	regulates	MGI:101893	Pou5f1	RV:0000006	regulates	MGI:98658	Tdgf1	Not specified	IEA	J:67890	jlewis		Mined from GAF file
"""

###--- Globals ---###

regGenerator = KeyGenerator.KeyGenerator('marker_regulated_marker')

###--- Functions ---###

# marker key -> (genetic chrom, genomic chrom, start coord, end coord)
coordCache = {}	

def populateCache():
	# populate the global 'coordCache' with location data for markers

	global coordCache

	cmd = '''select _Marker_key, genomicChromosome, chromosome,
			startCoordinate, endCoordinate
		from mrk_location_cache'''

	(cols, rows) = dbAgnostic.execute(cmd)

	keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
	genomicChrCol = dbAgnostic.columnNumber(cols, 'genomicChromosome')
	geneticChrCol = dbAgnostic.columnNumber(cols, 'chromosome')
	startCol = dbAgnostic.columnNumber(cols, 'startCoordinate')
	endCol = dbAgnostic.columnNumber(cols, 'endCoordinate')

	for row in rows:
		coordCache[row[keyCol]] = (row[geneticChrCol],
			row[genomicChrCol], row[startCol], row[endCol])

	logger.debug ('Cached %d locations' % len(coordCache))
	return

def getMarkerCoords(marker):
	# get (genetic chrom, genomic chrom, start coord, end coord) for the
	# given marker key or ID

	if len(coordCache) == 0:
		populateCache()

	if type(marker) == types.StringType:
		markerKey = keyLookup(marker, 2)
	else:
		markerKey = marker

	if coordCache.has_key(markerKey):
		return coordCache[markerKey]

	return (None, None, None, None)

def getChromosome (marker):
	# get the chromosome for the given marker key or ID, preferring
	# the genomic one over the genetic one

	(geneticChr, genomicChr, startCoord, endCoord) = getMarkerCoords(marker)

	if genomicChr:
		return genomicChr
	return geneticChr

def getStartCoord (marker):
	return getMarkerCoords(marker)[2]

def getEndCoord (marker):
	return getMarkerCoords(marker)[3] 

keyCache = {}

def keyLookup (accID, mgiType):
	key = (accID, mgiType)

	if not keyCache.has_key(key):
		cmd = '''select _Object_key
			from acc_accession
			where accID = '%s'
				and _MGIType_key = %d''' % (accID, mgiType)

		(cols, rows) = dbAgnostic.execute(cmd)

		if not rows:
			# fake reference for now (with test data)
			if mgiType == 1:
				keyCache[key] = 82823
			else:
				keyCache[key] = None 
		else:
			keyCache[key] = rows[0][0]

	return keyCache[key]
	
def slicedData():
	# provides SampleData, broken down into rows and columns.
	# replaces 'Add' column with row number; adds columns for marker keys
	# and reference key.

	rows = []
	for row in SampleData.split('\n'):
		if (row.strip() == ''):
			continue

		rows.append (row.split('\t'))
		rows[-1][0] = str(len(rows))

		# key of organizing marker (col 14)
		rows[-1].append (keyLookup (rows[-1][2], 2))

		# key of participant marker (col 15)
		rows[-1].append (keyLookup (rows[-1][6], 2))

		# reference key (col 16)
		rows[-1].append (keyLookup (rows[-1][10], 1))

	return rows

def extractColumns (row, columnNumbers):
	r = []
	for c in columnNumbers:
		r.append(row[c])
	return r

def emulateQuery0():
	# parse SampleData and give results as if data came from query 0

	cols = [ '_Relationship_key', 'relationship_category', 'marker_key',
		'regulated_marker_key', 'regulated_marker_symbol',
		'regulated_marker_id', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id' ]

	rows = []
	for row in slicedData():
		rows.append (extractColumns (row,
			[ 0, 1, 14, 15, 7, 6, 5, 8, 9, 16, 10 ]) )

	return cols, rows

def emulateQuery1():
	# parse SampleData and give results as if data came from query 1

	cols = [ '_Relationship_key', 'relationship_category', 'marker_key',
		'regulated_marker_key', 'regulated_marker_symbol',
		'regulated_marker_id', 'relationship_term', 'qualifier',
		'evidence_code', 'reference_key', 'jnum_id' ]

	rows = []
	for row in slicedData():
		rows.append (extractColumns (row,
			[ 0, 1, 15, 14, 3, 2, 5, 8, 9, 16, 10 ]) )

	for row in rows:
		if row[6] == 'regulates':
			row[6] = 'is_regulated_by'
		elif row[6] == 'miRNA silences':
			row[6] = 'is_silenced_by_miRNA'

	return cols, rows

def emulateQuery2():
	# parse SampleData and give results as if data came from query 2

	rows = []
	i = 0
	for row in slicedData():
		[ relationshipKey, note ] = extractColumns (row, [0, 13])

		if note:
			i = i + 1
			row = [ relationshipKey, note, 'note', i ]
			rows.append (row)

		if len(row) < 13:
			logger.debug('len(row) == %d' % len(row))
			continue

		properties = row[12]

		if not properties:
			logger.debug('no properties')
			continue
		if properties.strip() == '':
			logger.debug('empty properties')
			continue

		# properties string has semicolon-separated name=value pairs
		propLists = map(lambda x: x.split('='), properties.split(';'))

		for [ propName, propValue ] in propLists:
			i = i + 1
			row = [ relationshipKey, propValue, propName, i ]
			rows.append (row) 

	cols = [ '_Relationship_key', 'value', 'name', 'sequenceNum' ]

	return cols, rows

###--- Classes ---###

class RegGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the Template table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Templates,
	#	collates results, writes tab-delimited text file

	def regKey (self, relationshipKey, reverse = False):
		key = (relationshipKey, reverse)

		if not self.regMap.has_key(key):
			self.regMap[key] = len(self.regMap) + 1

		return self.regMap[key]

	def processQuery0 (self):
		# query 0 : basic marker-to-marker relationships

		global regGenerator

		cols, rows = self.results[0]

		# add chromosome and start coordinate fields to each row

		cols.append ('chromosome')
		cols.append ('startCoordinate')

		relMrkCol = Gatherer.columnNumber (cols, 'regulated_marker_key')
		for row in rows:
			row.append(getChromosome(row[relMrkCol]))
			row.append(getStartCoord(row[relMrkCol]))

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on regulated marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols,
			'regulated_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols, 'relationship_term')
		chrCol = Gatherer.columnNumber (cols, 'chromosome')
		coordCol = Gatherer.columnNumber (cols, 'startCoordinate')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(chrCol, ListSorter.CHROMOSOME),
			(coordCol, ListSorter.NUMERIC) ] )

		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 0 rows' % len(rows))

		# add reg_key field and sequence number field to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key') 
		cols.append ('reg_key')
		cols.append ('sequence_num')
		seqNum = 0

		for row in rows:
			row.append (regGenerator.getKey((row[relKeyCol], 0)))
			seqNum = seqNum + 1 
			row.append (seqNum)

		return cols, rows

	def processQuery1 (self, query1Cols):
		# query 1 : reversed marker-to-marker relationships

		cols1, rows1 = self.results[1]

		# assume cols == cols1; if not, we need to map them (to do)

		for c in cols1:
			if (cols1.index(c) != query1Cols.index(c)):
				raise 'error', 'List indexes differ'

		# add chromosome and start coordinate fields to each row

		cols1.append ('chromosome')
		cols1.append ('startCoordinate')

		relMrkCol = Gatherer.columnNumber (cols1, 'regulated_marker_key')
		for row in rows1:
			row.append(getChromosome(row[relMrkCol]))
			row.append(getStartCoord(row[relMrkCol]))

		# update sorting of rows to group by marker key, relationship
		# category, and a smart alpha sort on regulated marker symbol

		mrkKeyCol = Gatherer.columnNumber (cols1, 'marker_key')
		relSymbolCol = Gatherer.columnNumber (cols1,
			'regulated_marker_symbol')
		categoryCol = Gatherer.columnNumber (cols1,
			'relationship_category')
		termCol = Gatherer.columnNumber (cols1, 'relationship_term')
		chrCol = Gatherer.columnNumber (cols1, 'chromosome')
		coordCol = Gatherer.columnNumber (cols1, 'startCoordinate')

		ListSorter.setSortBy ( [ (mrkKeyCol, ListSorter.NUMERIC),
			(categoryCol, ListSorter.ALPHA),
			(termCol, ListSorter.ALPHA),
			(chrCol, ListSorter.CHROMOSOME),
			(coordCol, ListSorter.NUMERIC) ] )

		rows1.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 1 rows' % len(rows1))

		# add reg_key field and sequence number field to each row

		cols1.append ('reg_key')
		cols1.append ('sequence_num')

		relKeyCol = Gatherer.columnNumber (cols1, '_Relationship_key')

		seqNum = 0
		for row in rows1:
			row.append (regGenerator.getKey((row[relKeyCol], 1)))

			seqNum = seqNum + 1
			row.append (seqNum)

		return cols1, rows1

	def processQuery2 (self):
		# query 2 : properties for marker-to-marker relationships

		cols, rows = self.results[2]

		# add reg_key to each row

		relKeyCol = Gatherer.columnNumber (cols, '_Relationship_key')

		cols.append ('reg_key')

		for row in rows:
			row.append (regGenerator.getKey((row[relKeyCol], 0)))

		# sort rows to be ordered by reg_key, property name, and
		# property value

		regKeyCol = Gatherer.columnNumber (cols, 'reg_key')
		nameCol = Gatherer.columnNumber (cols, 'name')
		valueCol = Gatherer.columnNumber (cols, 'value')

		ListSorter.setSortBy ( [
			(regKeyCol, ListSorter.NUMERIC),
			(nameCol, ListSorter.ALPHA),
			(valueCol, ListSorter.SMART_ALPHA)
			] )
		rows.sort (ListSorter.compare)
		logger.debug ('Sorted %d query 2 rows' % len(rows))

		# add sequence number to each row

		cols.append ('sequence_num')

		seqNum = 0
		for row in rows:
			seqNum = seqNum + 1
			row.append (seqNum)

		return cols, rows

	def processQuery3 (self):
		return [], []

	def processQuery4 (self):
		return [], []

	def processQuery5 (self):
		return [], []

	def collateResults (self):
		self.results = [ emulateQuery0(), emulateQuery1(),
			emulateQuery2() ]

		self.regMap = {}	# maps _Relationship_key to reg_key

		cols, rows = self.processQuery0()
		cols1, rows1 = self.processQuery1(cols)

		logger.debug ('Found %d rows for queries 0-1' % (
			len(rows) + len(rows1)) )
		self.output.append ( (cols, rows + rows1) )

		cols, rows = self.processQuery2()

		# query 3 : properties for reversed mrk-to-mrk relationships

		# query 4 : notes for mrk-to-mrk relationships

		# query 5 : notes for reversed mrk-to-mrk relationships

		logger.debug ('Found %d rows for queries 2-5' % len(rows))
		self.output.append ( (cols, rows) )

		return

###--- globals ---###

cmds = []

cmds2 = [
	# 0. basic marker-to-marker relationship data
	'''select r._Relationship_key,
			c.name as relationship_category,
			r._Object_key_1 as marker_key,
			r._Object_key_2 as regulated_marker_key,
			m.symbol as regulated_marker_symbol,
			t.term as relationship_term,
			a.accID as regulated_marker_id,
			q.term as qualifier,
			e.term as evidence_code,
			bc._Refs_key as reference_key,
			bc.jnum_id
		from mgi_relationship_category c,
			mgi_relationship r,
			mrk_marker m,
			voc_term t,
			acc_accession a,
			voc_term q,
			voc_term e,
			bib_citation_cache bc
		where c._Category_key = r._Category_key
			and c._MGIType_key_1 = 2
			and c._MGIType_key_2 = 2
			and r._Object_key_2 = m._Marker_key
			and r._RelationshipTerm_key = t._Term_key
			and m._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a._LogicalDB_key = 1
			and a.preferred = 1
			and r._Qualifier_key = q._Term_key
			and r._Evidence_key = e._Term_key
			and r._Refs_key = bc._Refs_key
		order by r._Object_key_1''',

	# 1. reversed marker-to-marker relationship data
	'''TBD
		''',

	# 2. properties
	'''select _Relationship_key,
			name,
			value,
			sequenceNum
		from mgi_relationship_property
		order by _Relationship_key, sequenceNum''', 

	# 3. properties for reverse relationships
	'''TBD
		''',

	# 4. relationship notes (if needed for display)
	'''TBD
		''',

	# 5. relationship notes (if needed for display)
	'''TBD
		''',

	]

# prefix for the filename of the output file
files = [
	('marker_regulated_marker',
		[ 'reg_key', 'marker_key', 'regulated_marker_key',
			'regulated_marker_symbol', 'regulated_marker_id',
			'relationship_category', 'relationship_term',
			'qualifier', 'evidence_code', 'reference_key',
			'jnum_id', 'sequence_num', ],
		'marker_regulated_marker'),

	('marker_reg_property',
		[ Gatherer.AUTO, 'reg_key', 'name', 'value', 'sequence_num' ],
		'marker_reg_property'),
	]

# global instance of a RegGatherer
gatherer = RegGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
