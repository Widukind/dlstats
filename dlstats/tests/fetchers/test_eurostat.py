# -*- coding: utf-8 -*-

import tempfile
import datetime
import os
from copy import deepcopy
from pprint import pprint
from urllib.parse import urlparse
from urllib.request import url2pathname, pathname2url

from dlstats.fetchers.eurostat import Eurostat as Fetcher

from dlstats.fetchers._commons import Datasets
from dlstats.fetchers import eurostat
from dlstats import constants

import unittest
from unittest import mock
import httpretty

from dlstats.tests.base import RESOURCES_DIR as BASE_RESOURCES_DIR, BaseTestCase, BaseDBTestCase
from dlstats.tests.fetchers.base import BaseFetcherTestCase, body_generator
from dlstats.tests.resources import xml_samples

RESOURCES_DIR = os.path.abspath(os.path.join(BASE_RESOURCES_DIR, "eurostat"))
TOC_FP = os.path.abspath(os.path.join(RESOURCES_DIR, "table_of_contents.xml"))

# Nombre de série dans les exemples
SERIES_COUNT = 1

PROVIDER_NAME = 'Eurostat'

DATASETS = {'nama_10_gdp': {},
            'bop_c6_q': {},
            'bop_c6_m': {}}

#---Dataset nama_10_gdp
DATASETS['nama_10_gdp']["dimensions_count"] = 4 
DATASETS['nama_10_gdp']["name"] = "nama_10_gdp"
DATASETS['nama_10_gdp']["doc_href"] = None
DATASETS['nama_10_gdp']["last_update"] = datetime.datetime(2015,10,26)
DATASETS['nama_10_gdp']["filename"] = "nama_10_gdp"
DATASETS['nama_10_gdp']["series_count"] = 1
DATASETS['nama_10_gdp']["sdmx"] = """<?xml version="1.0" encoding="UTF-8"?>
<CompactData xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:data="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:nama_10_gdp_DSD:compact" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:nama_10_gdp_DSD:compact EUROSTAT_nama_10_gdp_Compact.xsd http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact SDMXCompactData.xsd">
<Header>
<ID>nama_10_gdp</ID>
<Test>false</Test>
<Name xml:lang="en">nama_10_gdp</Name>
<Prepared>2015-10-26T21:08:39</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">SDMX-ML File</Name>
</Receiver>
<DataSetID>nama_10_gdp</DataSetID>
<Extracted>2015-10-26T21:08:39</Extracted>
</Header>
<data:DataSet>
<data:Series FREQ="A" unit="CLV05_MEUR" na_item="B1G" geo="AT" TIME_FORMAT="P1Y">
<data:Obs TIME_PERIOD="1995" OBS_VALUE="176840.7" />
</data:Series>
</data:DataSet>
</CompactData>
"""

DATASETS['nama_10_gdp']["dsd"] = """<?xml version="1.0" encoding="UTF-8"?>
<Structure xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:cross="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross" xmlns:generic="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic" xmlns:query="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/query" xmlns:structure="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure" xmlns:utility="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/utility" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd">
<Header>
<ID>nama_10_gdp_DSD</ID>
<Test>false</Test>
<Truncated>false</Truncated>
<Name xml:lang="en">nama_10_gdp_DSD</Name>
<Prepared>2015-10-26T21:08:39</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">XML File</Name>
</Receiver>
</Header>

<CodeLists>
<structure:CodeList id="CL_UNIT" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Unit of measure</structure:Name>
<structure:Name xml:lang="de">Maßeinheit</structure:Name>
<structure:Name xml:lang="fr">Unité de mesure</structure:Name>
<structure:Code value="CLV05_MEUR">
<structure:Description xml:lang="en">Chain linked volumes (2005), million euro</structure:Description>
<structure:Description xml:lang="de">Verkettete Volumen (2005), Millionen Euro</structure:Description>
<structure:Description xml:lang="fr">Volumes chaînés (2005), millions d'euros</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_NA_ITEM" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">National accounts indicator (ESA10)</structure:Name>
<structure:Name xml:lang="de">Volkswirtschaftliche Gesamtrechnungen Indikator (ESVG10)</structure:Name>
<structure:Name xml:lang="fr">Indicateur des comptes nationaux (SEC10)</structure:Name>
<structure:Code value="B1G">
<structure:Description xml:lang="en">Value added, gross</structure:Description>
<structure:Description xml:lang="de">Bruttowertschöpfung</structure:Description>
<structure:Description xml:lang="fr">Valeur ajoutée, brute</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_GEO" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
<structure:Name xml:lang="de">Geopolitische Meldeeinheit</structure:Name>
<structure:Name xml:lang="fr">Entité géopolitique (déclarante)</structure:Name>
<structure:Code value="AT">
<structure:Description xml:lang="en">Austria</structure:Description>
<structure:Description xml:lang="de">Österreich</structure:Description>
<structure:Description xml:lang="fr">Autriche</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_TIME_FORMAT" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Time Format</structure:Name>
<structure:Code value="P1Y">
<structure:Description xml:lang="en">Annual</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_FREQ" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Frequency code list</structure:Name>
<structure:Code value="A">
<structure:Description xml:lang="en">Annual</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_OBS_STATUS" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Observation status code list</structure:Name>
<structure:Code value="p">
<structure:Description xml:lang="en">provisional</structure:Description>
<structure:Description xml:lang="de">vorläufig</structure:Description>
<structure:Description xml:lang="fr">provisoire</structure:Description>
</structure:Code>
</structure:CodeList>
</CodeLists>

<Concepts>
<structure:ConceptScheme agencyID="EUROSTAT" id="CONCEPTS" isFinal="true">
<structure:Name xml:lang="en">Concepts</structure:Name>
<structure:Concept id="FREQ"><structure:Name xml:lang="en">Frequency</structure:Name>
</structure:Concept>
<structure:Concept id="unit"><structure:Name xml:lang="en">Unit of measure</structure:Name>
<structure:Name xml:lang="de">Maßeinheit</structure:Name>
<structure:Name xml:lang="fr">Unité de mesure</structure:Name>
</structure:Concept>
<structure:Concept id="na_item"><structure:Name xml:lang="en">National accounts indicator (ESA10)</structure:Name>
<structure:Name xml:lang="de">Volkswirtschaftliche Gesamtrechnungen Indikator (ESVG10)</structure:Name>
<structure:Name xml:lang="fr">Indicateur des comptes nationaux (SEC10)</structure:Name>
</structure:Concept>
<structure:Concept id="geo"><structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
<structure:Name xml:lang="de">Geopolitische Meldeeinheit</structure:Name>
<structure:Name xml:lang="fr">Entité géopolitique (déclarante)</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_PERIOD"><structure:Name xml:lang="en">Time period or range</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_VALUE"><structure:Name xml:lang="en">Observation Value</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_STATUS"><structure:Name xml:lang="en">Observation Status</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_FORMAT"><structure:Name xml:lang="en">Time Format</structure:Name>
</structure:Concept>
</structure:ConceptScheme>
</Concepts>

<KeyFamilies>
<structure:KeyFamily id="nama_10_gdp_DSD" agencyID="EUROSTAT" isFinal="true" isExternalReference="false"><structure:Name xml:lang="en">nama_10_gdp_DSD</structure:Name>

<structure:Components>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="FREQ" codelistAgency="SDMX" codelist="CL_FREQ" isFrequencyDimension="true"/>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="unit" codelistAgency="ESTAT" codelist="CL_UNIT" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="na_item" codelistAgency="ESTAT" codelist="CL_NA_ITEM" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="geo" codelistAgency="ESTAT" codelist="CL_GEO" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:TimeDimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="TIME_PERIOD"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:TimeDimension>
<structure:PrimaryMeasure conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_VALUE">
<structure:TextFormat textType="Double"></structure:TextFormat>
</structure:PrimaryMeasure>

<structure:Attribute conceptRef="TIME_FORMAT" conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" codelistAgency="SDMX" codelist="CL_TIME_FORMAT" attachmentLevel="Series" assignmentStatus="Mandatory"></structure:Attribute>
<structure:Attribute conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_STATUS" codelistAgency="EUROSTAT" codelist="CL_OBS_STATUS" attachmentLevel="Observation" assignmentStatus="Conditional" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="false" crossSectionalAttachObservation="true"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:Attribute>
</structure:Components>
</structure:KeyFamily>
</KeyFamilies>

</Structure>
"""

DATASETS['bop_c6_m']["dimensions_count"] = 8 
DATASETS['bop_c6_m']["name"] = "bop_c6_m"
DATASETS['bop_c6_m']["doc_href"] = None
DATASETS['bop_c6_m']["last_update"] = datetime.datetime(2015,11,20)
DATASETS['bop_c6_m']["filename"] = "bop_c6_m"
DATASETS['bop_c6_m']["series_count"] = 1
DATASETS['bop_c6_m']["sdmx"] = """<?xml version="1.0" encoding="UTF-8"?>
<CompactData xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:data="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:bop_c6_m_DSD:compact" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:bop_c6_m_DSD:compact EUROSTAT_bop_c6_m_Compact.xsd http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact SDMXCompactData.xsd">
<Header>
<ID>bop_c6_m</ID>
<Test>false</Test>
<Name xml:lang="en">bop_c6_m</Name>
<Prepared>2015-11-20T09:12:22</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">SDMX-ML File</Name>
</Receiver>
<DataSetID>bop_c6_m</DataSetID>
<Extracted>2015-11-20T09:12:22</Extracted>
</Header>
<data:DataSet>
<data:Series FREQ="M" currency="MIO_EUR" bop_item="CA" sector10="S1" sectpart="S1" stk_flow="BAL" partner="EA18" geo="MT" TIME_FORMAT="P1M">
<data:Obs TIME_PERIOD="2013-01" OBS_STATUS="c" />
</data:Series>
</data:DataSet>
</CompactData>
"""
DATASETS['bop_c6_m']["dsd"] = """<?xml version="1.0" encoding="UTF-8"?>
<Structure xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:cross="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross" xmlns:generic="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic" xmlns:query="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/query" xmlns:structure="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure" xmlns:utility="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/utility" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd">
<Header>
<ID>bop_c6_m_DSD</ID>
<Test>false</Test>
<Truncated>false</Truncated>
<Name xml:lang="en">bop_c6_m_DSD</Name>
<Prepared>2015-11-20T09:12:22</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">XML File</Name>
</Receiver>
</Header>

<CodeLists>
<structure:CodeList id="CL_CURRENCY" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Currency</structure:Name>
<structure:Code value="MIO_EUR">
<structure:Description xml:lang="en">Million euro</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_BOP_ITEM" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">BOP_item</structure:Name>
<structure:Code value="CA">
<structure:Description xml:lang="en">Current account</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_SECTOR10" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Code value="S1">
<structure:Description xml:lang="en">Total economy</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_SECTPART" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Code value="S1">
<structure:Description xml:lang="en">Total economy</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_STK_FLOW" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Stock or flow</structure:Name>
<structure:Code value="BAL">
<structure:Description xml:lang="en">Balance</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_PARTNER" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Geopolitical entity (partner)</structure:Name>
<structure:Code value="EA18">
<structure:Description xml:lang="en">Euro area (18 countries)</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_GEO" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
<structure:Code value="MT">
<structure:Description xml:lang="en">Malta</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_TIME_FORMAT" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Time Format</structure:Name>
<structure:Code value="P1M">
<structure:Description xml:lang="en">Monthly</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_FREQ" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Frequency code list</structure:Name>
<structure:Code value="M">
<structure:Description xml:lang="en">Monthly</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_OBS_STATUS" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Observation status code list</structure:Name>
<structure:Code value="c">
<structure:Description xml:lang="en">confidential</structure:Description>
</structure:Code>
</structure:CodeList>
</CodeLists>

<Concepts>
<structure:ConceptScheme agencyID="EUROSTAT" id="CONCEPTS" isFinal="true">
<structure:Name xml:lang="en">Concepts</structure:Name>
<structure:Concept id="FREQ"><structure:Name xml:lang="en">Frequency</structure:Name>
</structure:Concept>
<structure:Concept id="currency"><structure:Name xml:lang="en">Currency</structure:Name>
</structure:Concept>
<structure:Concept id="bop_item"><structure:Name xml:lang="en">BOP_item</structure:Name>
</structure:Concept>
<structure:Concept id="sector10"><structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
</structure:Concept>
<structure:Concept id="sectpart"><structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
</structure:Concept>
<structure:Concept id="stk_flow"><structure:Name xml:lang="en">Stock or flow</structure:Name>
</structure:Concept>
<structure:Concept id="partner"><structure:Name xml:lang="en">Geopolitical entity (partner)</structure:Name>
</structure:Concept>
<structure:Concept id="geo"><structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_PERIOD"><structure:Name xml:lang="en">Time period or range</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_VALUE"><structure:Name xml:lang="en">Observation Value</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_STATUS"><structure:Name xml:lang="en">Observation Status</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_FORMAT"><structure:Name xml:lang="en">Time Format</structure:Name>
</structure:Concept>
</structure:ConceptScheme>
</Concepts>

<KeyFamilies>
<structure:KeyFamily id="bop_c6_m_DSD" agencyID="EUROSTAT" isFinal="true" isExternalReference="false"><structure:Name xml:lang="en">bop_c6_m_DSD</structure:Name>

<structure:Components>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="FREQ" codelistAgency="SDMX" codelist="CL_FREQ" isFrequencyDimension="true"/>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="currency" codelistAgency="ESTAT" codelist="CL_CURRENCY" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="bop_item" codelistAgency="ESTAT" codelist="CL_BOP_ITEM" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="sector10" codelistAgency="ESTAT" codelist="CL_SECTOR10" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="sectpart" codelistAgency="ESTAT" codelist="CL_SECTPART" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="stk_flow" codelistAgency="ESTAT" codelist="CL_STK_FLOW" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="partner" codelistAgency="ESTAT" codelist="CL_PARTNER" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="geo" codelistAgency="ESTAT" codelist="CL_GEO" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:TimeDimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="TIME_PERIOD"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:TimeDimension>
<structure:PrimaryMeasure conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_VALUE">
<structure:TextFormat textType="Double"></structure:TextFormat>
</structure:PrimaryMeasure>

<structure:Attribute conceptRef="TIME_FORMAT" conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" codelistAgency="SDMX" codelist="CL_TIME_FORMAT" attachmentLevel="Series" assignmentStatus="Mandatory"></structure:Attribute>
<structure:Attribute conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_STATUS" codelistAgency="EUROSTAT" codelist="CL_OBS_STATUS" attachmentLevel="Observation" assignmentStatus="Conditional" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="false" crossSectionalAttachObservation="true"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:Attribute>
</structure:Components>
</structure:KeyFamily>
</KeyFamilies>

</Structure>
"""

DATASETS['bop_c6_q']["dimensions_count"] = 8 
DATASETS['bop_c6_q']["name"] = "bop_c6_q"
DATASETS['bop_c6_q']["doc_href"] = None
DATASETS['bop_c6_q']["last_update"] = datetime.datetime(2015,11,20)
DATASETS['bop_c6_q']["filename"] = "bop_c6_q"
DATASETS['bop_c6_q']["series_count"] = 2
DATASETS['bop_c6_q']["sdmx"] = """<?xml version="1.0" encoding="UTF-8"?>
<CompactData xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:data="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:bop_c6_q_DSD:compact" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=EUROSTAT:bop_c6_q_DSD:compact EUROSTAT_bop_c6_q_Compact.xsd http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact SDMXCompactData.xsd">
<Header>
<ID>bop_c6_q</ID>
<Test>false</Test>
<Name xml:lang="en">bop_c6_q</Name>
<Prepared>2015-11-16T13:49:33</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">SDMX-ML File</Name>
</Receiver>
<DataSetID>bop_c6_q</DataSetID>
<Extracted>2015-11-16T13:49:33</Extracted>
</Header>
<data:DataSet>
<data:Series FREQ="A" currency="MIO_EUR" bop_item="CA" sector10="S1" sectpart="S1" stk_flow="BAL" partner="AT" geo="MT" TIME_FORMAT="P1Y">
<data:Obs TIME_PERIOD="2004" OBS_STATUS="c" />
<data:Obs TIME_PERIOD="2005" OBS_STATUS="c" />
<data:Obs TIME_PERIOD="2006" OBS_STATUS="c" />
<data:Obs TIME_PERIOD="2007" OBS_STATUS="c" />
<data:Obs TIME_PERIOD="2008" OBS_VALUE="-201.4" />
</data:Series>
<data:Series FREQ="Q" currency="MIO_EUR" bop_item="CA" sector10="S1" sectpart="S1" stk_flow="BAL" partner="AT" geo="MT" TIME_FORMAT="P3M">
<data:Obs TIME_PERIOD="2004-Q1" OBS_STATUS="c" />
</data:Series>
</data:DataSet>
</CompactData>
"""
DATASETS['bop_c6_q']["dsd"] = """<Structure xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message" xmlns:common="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/common" xmlns:compact="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/compact" xmlns:cross="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/cross" xmlns:generic="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic" xmlns:query="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/query" xmlns:structure="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure" xmlns:utility="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/utility" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd">
<Header>
<ID>bop_c6_q_DSD</ID>
<Test>false</Test>
<Truncated>false</Truncated>
<Name xml:lang="en">bop_c6_q_DSD</Name>
<Prepared>2015-11-16T13:49:32</Prepared>
<Sender id="EUROSTAT">
<Name xml:lang="en">EUROSTAT</Name>
</Sender>
<Receiver id="XML">
<Name xml:lang="en">XML File</Name>
</Receiver>
</Header>

<CodeLists>
<structure:CodeList id="CL_CURRENCY" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Currency</structure:Name>
<structure:Code value="MIO_EUR">
<structure:Description xml:lang="en">Million euro</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_BOP_ITEM" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">BOP_item</structure:Name>
<structure:Code value="CA">
<structure:Description xml:lang="en">Current account</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_SECTOR10" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Code value="S1">
<structure:Description xml:lang="en">Total economy</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_SECTPART" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Code value="S1">
<structure:Description xml:lang="en">Total economy</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_STK_FLOW" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Stock or flow</structure:Name>
<structure:Code value="BAL">
<structure:Description xml:lang="en">Balance</structure:Description>
<structure:Description xml:lang="de">Saldo</structure:Description>
<structure:Description xml:lang="fr">Solde</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_PARTNER" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Geopolitical entity (partner)</structure:Name>
<structure:Code value="AT">
<structure:Description xml:lang="en">Austria</structure:Description>
</structure:Code>
<structure:Code value="PL">
<structure:Description xml:lang="en">Poland</structure:Description>
<structure:Description xml:lang="de">Polen</structure:Description>
<structure:Description xml:lang="fr">Pologne</structure:Description>
</structure:Code>
<structure:Code value="PT">
<structure:Description xml:lang="en">Portugal</structure:Description>
<structure:Description xml:lang="de">Portugal</structure:Description>
<structure:Description xml:lang="fr">Portugal</structure:Description>
</structure:Code>
<structure:Code value="RO">
<structure:Description xml:lang="en">Romania</structure:Description>
<structure:Description xml:lang="de">Rumänien</structure:Description>
<structure:Description xml:lang="fr">Roumanie</structure:Description>
</structure:Code>
<structure:Code value="SI">
<structure:Description xml:lang="en">Slovenia</structure:Description>
<structure:Description xml:lang="de">Slowenien</structure:Description>
<structure:Description xml:lang="fr">Slovénie</structure:Description>
</structure:Code>
<structure:Code value="SK">
<structure:Description xml:lang="en">Slovakia</structure:Description>
<structure:Description xml:lang="de">Slowakei</structure:Description>
<structure:Description xml:lang="fr">Slovaquie</structure:Description>
</structure:Code>
<structure:Code value="FI">
<structure:Description xml:lang="en">Finland</structure:Description>
<structure:Description xml:lang="de">Finnland</structure:Description>
<structure:Description xml:lang="fr">Finlande</structure:Description>
</structure:Code>
<structure:Code value="SE">
<structure:Description xml:lang="en">Sweden</structure:Description>
<structure:Description xml:lang="de">Schweden</structure:Description>
<structure:Description xml:lang="fr">Suède</structure:Description>
</structure:Code>
<structure:Code value="UK">
<structure:Description xml:lang="en">United Kingdom</structure:Description>
<structure:Description xml:lang="de">Vereinigtes Königreich</structure:Description>
<structure:Description xml:lang="fr">Royaume-Uni</structure:Description>
</structure:Code>
<structure:Code value="CH">
<structure:Description xml:lang="en">Switzerland</structure:Description>
<structure:Description xml:lang="de">Schweiz</structure:Description>
<structure:Description xml:lang="fr">Suisse</structure:Description>
</structure:Code>
<structure:Code value="RU">
<structure:Description xml:lang="en">Russia</structure:Description>
<structure:Description xml:lang="de">Russland</structure:Description>
<structure:Description xml:lang="fr">Russie</structure:Description>
</structure:Code>
<structure:Code value="EXT_EU28">
<structure:Description xml:lang="en">Extra EU-28</structure:Description>
<structure:Description xml:lang="de">Extra-EU-28</structure:Description>
<structure:Description xml:lang="fr">Extra-UE-28</structure:Description>
</structure:Code>
<structure:Code value="EXT_EA19">
<structure:Description xml:lang="en">Extra-Euro area-19</structure:Description>
<structure:Description xml:lang="de">Extra-Euroraum (19)</structure:Description>
<structure:Description xml:lang="fr">Extra-zone euro à 19 pays</structure:Description>
</structure:Code>
<structure:Code value="CA">
<structure:Description xml:lang="en">Canada</structure:Description>
<structure:Description xml:lang="de">Kanada</structure:Description>
<structure:Description xml:lang="fr">Canada</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_GEO" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
<structure:Code value="MT">
<structure:Description xml:lang="en">Malta</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_TIME_FORMAT" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Time Format</structure:Name>
<structure:Code value="P1Y">
<structure:Description xml:lang="en">Annual</structure:Description>
</structure:Code>
<structure:Code value="P3M">
<structure:Description xml:lang="en">Quarterly</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_FREQ" agencyID="SDMX" isFinal="true">
<structure:Name xml:lang="en">Frequency code list</structure:Name>
<structure:Code value="A">
<structure:Description xml:lang="en">Annual</structure:Description>
</structure:Code>
<structure:Code value="Q">
<structure:Description xml:lang="en">Quarterly</structure:Description>
</structure:Code>
</structure:CodeList>
<structure:CodeList id="CL_OBS_STATUS" agencyID="EUROSTAT" isFinal="true">
<structure:Name xml:lang="en">Observation status code list</structure:Name>
<structure:Code value="c">
<structure:Description xml:lang="en">confidential</structure:Description>
</structure:Code>
</structure:CodeList>
</CodeLists>

<Concepts>
<structure:ConceptScheme agencyID="EUROSTAT" id="CONCEPTS" isFinal="true">
<structure:Name xml:lang="en">Concepts</structure:Name>
<structure:Concept id="FREQ"><structure:Name xml:lang="en">Frequency</structure:Name>
</structure:Concept>
<structure:Concept id="currency"><structure:Name xml:lang="en">Currency</structure:Name>
<structure:Name xml:lang="de">Währung</structure:Name>
<structure:Name xml:lang="fr">Monnaie</structure:Name>
</structure:Concept>
<structure:Concept id="bop_item"><structure:Name xml:lang="en">BOP_item</structure:Name>
<structure:Name xml:lang="de">ZB_Position</structure:Name>
<structure:Name xml:lang="fr">BDP_poste</structure:Name>
</structure:Concept>
<structure:Concept id="sector10"><structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Name xml:lang="de">Sektor (ESWG10)</structure:Name>
<structure:Name xml:lang="fr">Secteur (SEC 10)</structure:Name>
</structure:Concept>
<structure:Concept id="sectpart"><structure:Name xml:lang="en">Sector (ESA10)</structure:Name>
<structure:Name xml:lang="de">Sektor (ESWG10)</structure:Name>
<structure:Name xml:lang="fr">Secteur (SEC10)</structure:Name>
</structure:Concept>
<structure:Concept id="stk_flow"><structure:Name xml:lang="en">Stock or flow</structure:Name>
<structure:Name xml:lang="de">Bestand oder Fluss</structure:Name>
<structure:Name xml:lang="fr">Stock ou flux</structure:Name>
</structure:Concept>
<structure:Concept id="partner"><structure:Name xml:lang="en">Geopolitical entity (partner)</structure:Name>
<structure:Name xml:lang="de">Geopolitische Partnereinheit</structure:Name>
<structure:Name xml:lang="fr">Entité géopolitique (partenaire)</structure:Name>
</structure:Concept>
<structure:Concept id="geo"><structure:Name xml:lang="en">Geopolitical entity (reporting)</structure:Name>
<structure:Name xml:lang="de">Geopolitische Meldeeinheit</structure:Name>
<structure:Name xml:lang="fr">Entité géopolitique (déclarante)</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_PERIOD"><structure:Name xml:lang="en">Time period or range</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_VALUE"><structure:Name xml:lang="en">Observation Value</structure:Name>
</structure:Concept>
<structure:Concept id="OBS_STATUS"><structure:Name xml:lang="en">Observation Status</structure:Name>
</structure:Concept>
<structure:Concept id="TIME_FORMAT"><structure:Name xml:lang="en">Time Format</structure:Name>
</structure:Concept>
</structure:ConceptScheme>
</Concepts>

<KeyFamilies>
<structure:KeyFamily id="bop_c6_q_DSD" agencyID="EUROSTAT" isFinal="true" isExternalReference="false"><structure:Name xml:lang="en">bop_c6_q_DSD</structure:Name>

<structure:Components>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="FREQ" codelistAgency="SDMX" codelist="CL_FREQ" isFrequencyDimension="true"/>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="currency" codelistAgency="ESTAT" codelist="CL_CURRENCY" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="bop_item" codelistAgency="ESTAT" codelist="CL_BOP_ITEM" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="sector10" codelistAgency="ESTAT" codelist="CL_SECTOR10" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="sectpart" codelistAgency="ESTAT" codelist="CL_SECTPART" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="stk_flow" codelistAgency="ESTAT" codelist="CL_STK_FLOW" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="partner" codelistAgency="ESTAT" codelist="CL_PARTNER" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:Dimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="geo" codelistAgency="ESTAT" codelist="CL_GEO" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="true" crossSectionalAttachObservation="false"></structure:Dimension>
<structure:TimeDimension conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="TIME_PERIOD"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:TimeDimension>
<structure:PrimaryMeasure conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_VALUE">
<structure:TextFormat textType="Double"></structure:TextFormat>
</structure:PrimaryMeasure>

<structure:Attribute conceptRef="TIME_FORMAT" conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" codelistAgency="SDMX" codelist="CL_TIME_FORMAT" attachmentLevel="Series" assignmentStatus="Mandatory"></structure:Attribute>
<structure:Attribute conceptSchemeRef="CONCEPTS" conceptSchemeAgency="EUROSTAT" conceptRef="OBS_STATUS" codelistAgency="EUROSTAT" codelist="CL_OBS_STATUS" attachmentLevel="Observation" assignmentStatus="Conditional" crossSectionalAttachDataSet="false" crossSectionalAttachGroup="false" crossSectionalAttachSection="false" crossSectionalAttachObservation="true"><structure:TextFormat textType="String"></structure:TextFormat>
</structure:Attribute>
</structure:Components>
</structure:KeyFamily>
</KeyFamilies>

</Structure>
"""



DATASETS['dset1'] = deepcopy(DATASETS['nama_10_gdp'])
DATASETS['dset1']["name"] = "dset1"
DATASETS['dset1']["filename"] = "dset1"

DATASETS['dset2'] = deepcopy(DATASETS['nama_10_gdp'])
DATASETS['dset2']["name"] = "dset2"
DATASETS['dset2']["filename"] = "dset2"

def make_url(self):
    filepath = os.path.abspath(os.path.join(tempfile.gettempdir(), 
                                            self.provider_name, 
                                            self.dataset_code,
                                            "tests",
                                            self.dataset_code+'.sdmx.zip'))
    return "file:%s" % pathname2url(filepath)

def local_get(url, *args, **kwargs):
    "Fetch a stream from local files."
    from requests import Response

    p_url = urlparse(url)
    if p_url.scheme != 'file':
        raise ValueError("Expected file scheme")

    filename = url2pathname(p_url.path)
    response = Response()
    response.status_code = 200
    response.raw = open(filename, 'rb')
    return response

def write_zip_file(zip_filepath, filename, dsd, sdmx):
    """Create file in zipfile
    """
    import zipfile

    with zipfile.ZipFile(zip_filepath, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename+'.dsd.xml', dsd)
        zf.writestr(filename+'.sdmx.xml', sdmx)
        
def get_filepath(dataset_code):
    """Create SDMX file in zipfile
    
    Return local filepath of zipfile
    """
    dataset = DATASETS[dataset_code]
    zip_filename = dataset_code+'.sdmx.zip'
    filename = dataset_code
    dirpath = os.path.join(tempfile.gettempdir(), PROVIDER_NAME, dataset_code, "tests")
    filepath = os.path.abspath(os.path.join(dirpath, zip_filename))
    
    if os.path.exists(filepath):
        os.remove(filepath)
        
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    
    write_zip_file(filepath, filename, DATASETS[dataset_code]['dsd'], DATASETS[dataset_code]['sdmx'])
    
    return filepath

def load_fake_datas(select_dataset_code=None):
    """Load datas from DATASETS dict
    
    key: DATASETS[dataset_code]['datas']
    """
    
    fetcher = eurostat.Eurostat()
    
    results = {}
    
    for dataset_code, dataset in DATASETS.items():
        
        if select_dataset_code and select_dataset_code != dataset_code:
            continue
        
        _dataset = Datasets(provider_name=fetcher.provider_name, 
                    dataset_code=dataset_code, 
                    name=dataset['name'], 
                    doc_href=dataset['doc_href'], 
                    fetcher=fetcher, 
                    is_load_previous_version=False)
        
        dataset_datas = eurostat.EurostatData(_dataset, is_autoload=False)
        dataset_datas._load_datas(dataset['datas'])
        
        results[dataset_code] = {'series': []}

        for d in dataset_datas.rows:
            results[dataset_code]['series'].append(dataset_datas.build_serie(d))
            
    #pprint(results)
    return results

def get_table_of_contents(self):
    return TOC_FP

def extract_zip_file(zipfilepath):
    import zipfile
    zfile = zipfile.ZipFile(zipfilepath)
    tmpfiledir = tempfile.mkdtemp()
    filepaths = {}
    for filename in zfile.namelist():
        filepaths.update({filename: zfile.extract(filename, 
                                                  os.path.abspath(tmpfiledir))})
    return filepaths

class FetcherTestCase(BaseFetcherTestCase):
    
    # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase
    
    FETCHER_KLASS = Fetcher
    DEBUG_MODE = False
    DATASETS = {
        'nama_10_fcs': deepcopy(xml_samples.DATA_EUROSTAT)
    }
    DATASET_FIRST = "bop_c6_m"
    DATASET_LAST = "nama_10_gdp"
    
    def _load_files(self, dataset_code=None):
        
        url = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml"
        self.register_url(url, 
                          TOC_FP,
                          match_querystring=True)
        
    @httpretty.activate
    def test_upsert_dataset_nama_10_fcs(self):
        
        # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase.test_upsert_dataset_nama_10_fcs

        dataset_code = "nama_10_fcs"

        dataset_zip_filepath = os.path.abspath(os.path.join(RESOURCES_DIR, "nama_10_fcs.sdmx.zip"))
        filepaths = extract_zip_file(dataset_zip_filepath)
        self.DATASETS[dataset_code]["DSD"]["filepaths"]["datastructure"] = filepaths['nama_10_fcs.dsd.xml']
        self.DATASETS[dataset_code]["filepath"] = filepaths['nama_10_fcs.sdmx.xml']
        
        url = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data/%s.sdmx.zip" % dataset_code
        self.register_url(url, 
                          dataset_zip_filepath,
                          match_querystring=True)
        
        self._load_files(dataset_code)
        
        self.assertProvider()
        self.assertDataTree(dataset_code)
        
        #self.fetcher.selected_codes = ['nama_10', 'cat1']
        self.fetcher.get_selected_datasets()
        self.assertEqual(len(self.fetcher.selected_datasets), 6)
        
        self.assertDataset(dataset_code)        
        #self.assertSeries(dataset_code)
        
    @httpretty.activate
    def test_data_tree(self):

        # nosetests -s -v dlstats.tests.fetchers.test_eurostat:FetcherTestCase.test_data_tree

        self._load_files()

        results = self.fetcher.upsert_data_tree()
        
        datasets_list = self.fetcher.datasets_list()

        self.assertEqual(len(datasets_list), 6)

        datasets = [
             {'dataset_code': 'bop_c6_m',
              'name': 'Balance of payments by country - monthly data (BPM6)',
              'last_update': datetime.datetime(2015, 10, 20, 0, 0),
              'metadata': {'data_end': '2015M08',
                            'data_start': '1991M01',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/bop_6_esms.htm',
                            'values': 4355217}},
             {'dataset_code': 'bop_c6_q',
              'name': 'Balance of payments by country - quarterly data (BPM6)',
              'last_update': datetime.datetime(2015, 10, 23, 0, 0),
              'metadata': {'data_end': '2015Q2',
                            'data_start': '1982',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/bop_6_esms.htm',
                            'values': 29844073}},
             {'dataset_code': 'dset1',
              'name': 'Dset1',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 417804}},
             {'dataset_code': 'dset2',
              'name': 'Dset2',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 69954}},
             {'dataset_code': 'nama_10_fcs',
              'name': 'Final consumption aggregates by durability',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 69954}},
             {'dataset_code': 'nama_10_gdp',
              'name': 'GDP and main components (output, expenditure and income)',
              'last_update': datetime.datetime(2015, 10, 26, 0, 0),
              'metadata': {'data_end': '2014',
                            'data_start': '1975',
                            'doc_href': 'http://ec.europa.eu/eurostat/cache/metadata/en/nama_10_esms.htm',
                            'values': 417804}}
        ]

        self.assertEqual(datasets_list, datasets)
        
