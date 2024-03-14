import os
from os.path import join, dirname
import time
import logging
# from dotenv import load_dotenv
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
time.sleep(10)
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='producer.log',
                    # filemode='w'
                    )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Table, Column, Integer, TEXT, JSON, TIMESTAMP, ForeignKey, Date, DateTime, Boolean
from sqlalchemy import create_engine

Base = declarative_base()

class Headers(Base):
    __tablename__ = 'headers'
    id = Column(Integer, primary_key=True)
    key = Column(TEXT)
    value = Column(TEXT)
    data_id = Column(Integer, ForeignKey('data.id'))
    data = relationship("Data", back_populates="headers")

class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    key = Column(TEXT)
    value = Column(JSON)
    timestamp = Column(TIMESTAMP, server_default="now()")
    offset = Column(Integer)
    topic = Column(TEXT)
    headers = relationship("Headers", back_populates="data")

association_table = Table(
    "association_table",
    Base.metadata,
    Column("ArbeitsunfaehigkeitsBescheinigung_Oid", ForeignKey("ArbeitsunfaehigkeitsBescheinigung.Oid"), primary_key=True),
    Column("Krankheitsursache_Icd", ForeignKey("Krankheitsursache.IcdCode"), primary_key=True),
)

class eAU(Base):
    __tablename__ = 'eAU'
    id = Column(Integer, primary_key=True)
    timeStamp = Column(TIMESTAMP, server_default="now()")
    offset = Column(Integer)
    _KafkaEventVersion = Column(TEXT)
    # ProzessBasisinformation
    prozessBasisinformation_id = Column(Integer, ForeignKey('prozessBasisinformation.id'))
    ProzessBasisinformation = relationship("ProzessBasisinformation")
    # ProzessPersonenInformation
    prozessPersonenInformation_id = Column(Integer, ForeignKey('prozessPersonenInformation.id'))
    ProzessPersonenInformation = relationship("ProzessPersonenInformation")
    # ArbeitsunfaehigkeitsBescheinigung
    ArbeitsunfaehigkeitsBescheinigung_id = Column(TEXT, ForeignKey('ArbeitsunfaehigkeitsBescheinigung.Oid'))
    ArbeitsunfaehigkeitsBescheinigung = relationship("ArbeitsunfaehigkeitsBescheinigung")

    def __init__(self, offset : int, message_value: dict):
        # assertions
        assert '_KafkaEventVersion' in message_value, "_KafkaEventVersion not in message"
        assert 'ProzessBasisinformation' in message_value, "ProzessBasisinformation not in message"
        assert 'ProzessPersonenInformation' in message_value, "ProzessPersonenInformation not in message"
        assert 'ArbeitsunfaehigkeitsBescheinigung' in message_value, "ArbeitsunfaehigkeitsBescheinigung not in message"
        
        self.offset = offset
        self._KafkaEventVersion = message_value['_KafkaEventVersion']
        # self.ProzessBasisinformation = ProzessBasisinformation(**message_value['ProzessBasisinformation'])
        # self.ProzessPersonenInformation = ProzessPersonenInformation(**message_value['ProzessPersonenInformation'])
        # self.ArbeitsunfaehigkeitsBescheinigung = ArbeitsunfaehigkeitsBescheinigung(**message_value['ArbeitsunfaehigkeitsBescheinigung'])
    
class ProzessBasisinformation(Base):
    __tablename__ = 'prozessBasisinformation'
    id = Column(Integer, primary_key=True)
    FallOid = Column(TEXT)
    InformationsKanal = Column(TEXT)
    InformationsQuelle = Column(TEXT)
    ProzessBezeichnung = Column(TEXT, nullable=False)
    ProzessschrittBezeichnung = Column(TEXT, nullable=False)
    ProzessschrittId = Column(TEXT, nullable=False)

    def __init__(self, values: dict):
        assert 'ProzessBezeichnung' in values, "ProzessBezeichnung not in message"
        assert 'ProzessschrittBezeichnung' in values, "ProzessschrittBezeichnung not in message"
        assert 'ProzessschrittId' in values, "ProzessschrittId not in message"

        self.ProzessBezeichnung = values['ProzessBezeichnung']
        self.ProzessschrittBezeichnung = values['ProzessschrittBezeichnung']
        self.ProzessschrittId = values['ProzessschrittId']

        if 'FallOid' in values:
            self.FallOid = values['FallOid']
        if 'InformationsKanal' in values:
            self.InformationsKanal = values['InformationsKanal']
        if 'InformationsQuelle' in values:
            self.InformationsQuelle = values['InformationsQuelle']

class ProzessPersonenInformation(Base):
    __tablename__ = 'prozessPersonenInformation'
    id = Column(Integer, primary_key=True)
    PartnerId = Column(TEXT)
    PartnerOid = Column(TEXT)
    VersichertePersonOid = Column(TEXT)
    VersichertePersonOrgEinheit = Column(TEXT)
    NatuerlichePersonOid = Column(TEXT)
    NatuerlichePersonSchutzstufe = Column(TEXT)

    def __init__(self, values: dict):
        if 'PartnerId' in values:
            self.PartnerId = values['PartnerId']
        if 'PartnerOid' in values:
            self.PartnerOid = values['PartnerOid']
        if 'VersichertePersonOid' in values:
            self.VersichertePersonOid = values['VersichertePersonOid']
        if 'VersichertePersonOrgEinheit' in values:
            self.VersichertePersonOrgEinheit = values['VersichertePersonOrgEinheit']
        if 'NatuerlichePersonOid' in values:
            self.NatuerlichePersonOid = values['NatuerlichePersonOid']
        if 'NatuerlichePersonSchutzstufe' in values:
            self.NatuerlichePersonSchutzstufe = values['NatuerlichePersonSchutzstufe']
class ArbeitsunfaehigkeitsBescheinigung(Base):
    __tablename__ = 'ArbeitsunfaehigkeitsBescheinigung'

    ArbeitsunfaehigBis = Column(Date)
    ArbeitsunfaehigVon = Column(Date)
    Arztname = Column(TEXT)
    Arztnummer = Column(TEXT)
    AusstellerTyp = Column(TEXT)
    Bemerkung = Column(TEXT)
    BescheinigungsTyp = Column(TEXT, nullable=False)
    BetriebsstaettenNummer = Column(TEXT)
    CreatedAt = Column(Date, nullable=False) # TODO: DateTimePattern: yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX
    CreatedBy = Column(TEXT, nullable=False)
    DokumentenId = Column(TEXT)
    ErfassungsDatum = Column(Date)
    FestgestelltAm = Column(Date, nullable=False)
    IstArbeitsunfall = Column(Boolean, nullable=False)
    IstEndbescheinigung = Column(Boolean, nullable=False)
    IstErstbescheinigung = Column(Boolean, nullable=False)
    IstKrankengeldFallGemeldet = Column(Boolean, nullable=False)
    IstMassnahmeAngelegt = Column(Boolean)
    IstMedizinischeReha = Column(Boolean, nullable=False)
    IstSonstigeMassnahme = Column(Boolean, nullable=False)
    SonstigeMassnahme = Column(TEXT)
    IstSonstigerUnfall = Column(Boolean, nullable=False)
    IstStornierungDurchgefuehrt = Column(Boolean)
    IstStufenweiseEingliederung = Column(Boolean, nullable=False)
    IstVersorgungsleiden = Column(Boolean, nullable=False)
    IstVonDurchgangsArztAusgestellt = Column(Boolean, nullable=False)
    # TODO: Krankheitsursache -> Diagnose
    Meldedatum = Column(Date)
    Oid = Column(TEXT, primary_key=True, nullable=False)
    StornierungsDatum = Column(Date)
    Krankheitsursachen = relationship("Krankheitsursache",
        secondary=association_table, back_populates="AUs"
    )

    def __init__(self, values: dict):
        # assert non nullable
        assert 'BescheinigungsTyp' in values, "BescheinigungsTyp not in message"
        assert 'CreatedAt' in values, "CreatedAt not in message"
        assert 'CreatedBy' in values, "CreatedBy not in message"
        assert 'FestgestelltAm' in values, "FestgestelltAm not in message"
        assert 'IstArbeitsunfall' in values, "IstArbeitsunfall not in message"
        assert 'IstEndbescheinigung' in values, "IstEndbescheinigung not in message"
        assert 'IstErstbescheinigung' in values, "IstErstbescheinigung not in message"
        assert 'IstKrankengeldFallGemeldet' in values, "IstKrankengeldFallGemeldet not in message"
        assert 'IstMedizinischeReha' in values, "IstMedizinischeReha not in message"
        assert 'IstSonstigeMassnahme' in values, "IstSonstigeMassnahme not in message"
        assert 'IstSonstigerUnfall' in values, "IstSonstigerUnfall not in message"
        assert 'IstStufenweiseEingliederung' in values, "IstStufenweiseEingliederung not in message"
        assert 'IstVersorgungsleiden' in values, "IstVersorgungsleiden not in message"
        assert 'IstVonDurchgangsArztAusgestellt' in values, "IstVonDurchgangsArztAusgestellt not in message"
        assert 'Oid' in values, "Oid not in message"

        # set values
        self.BescheinigungsTyp = values['BescheinigungsTyp']
        self.CreatedAt = values['CreatedAt']
        self.CreatedBy = values['CreatedBy']
        self.FestgestelltAm = values['FestgestelltAm']
        self.IstArbeitsunfall = values['IstArbeitsunfall']
        self.IstEndbescheinigung = values['IstEndbescheinigung']
        self.IstErstbescheinigung = values['IstErstbescheinigung']
        self.IstKrankengeldFallGemeldet = values['IstKrankengeldFallGemeldet']
        self.IstMedizinischeReha = values['IstMedizinischeReha']
        self.IstSonstigeMassnahme = values['IstSonstigeMassnahme']
        self.IstSonstigerUnfall = values['IstSonstigerUnfall']
        self.IstStufenweiseEingliederung = values['IstStufenweiseEingliederung']
        self.IstVersorgungsleiden = values['IstVersorgungsleiden']
        self.IstVonDurchgangsArztAusgestellt = values['IstVonDurchgangsArztAusgestellt']
        self.Oid = values['Oid']

        # set optional values
        if 'ArbeitsunfaehigBis' in values:
            self.ArbeitsunfaehigBis = values['ArbeitsunfaehigBis']
        if 'ArbeitsunfaehigVon' in values:
            self.ArbeitsunfaehigVon = values['ArbeitsunfaehigVon']
        if 'Arztname' in values:
            self.Arztname = values['Arztname']
        if 'Arztnummer' in values:
            self.Arztnummer = values['Arztnummer']
        if 'AusstellerTyp' in values:
            self.AusstellerTyp = values['AusstellerTyp']
        if 'Bemerkung' in values:
            self.Bemerkung = values['Bemerkung']
        if 'BetriebsstaettenNummer' in values:
            self.BetriebsstaettenNummer = values['BetriebsstaettenNummer']
        if 'ErfassungsDatum' in values:
            self.ErfassungsDatum = values['ErfassungsDatum']
        if 'IstMassnahmeAngelegt' in values:
            self.IstMassnahmeAngelegt = values['IstMassnahmeAngelegt']
        if 'SonstigeMassnahme' in values:
            self.SonstigeMassnahme = values['SonstigeMassnahme']
        if 'IstStornierungDurchgefuehrt' in values:
            self.IstStornierungDurchgefuehrt = values['IstStornierungDurchgefuehrt']
        if 'IstVersorgungsleiden' in values:
            self.IstVersorgungsleiden = values['IstVersorgungsleiden']
        if 'Meldedatum' in values:
            self.Meldedatum = values['Meldedatum']
        if 'StornierungsDatum' in values:
            self.StornierungsDatum = values['StornierungsDatum']


class Krankheitsursache(Base):
    __tablename__ = 'Krankheitsursache'
    IcdCode = Column(TEXT, primary_key = True, nullable=False)
    Qualifikation = Column(TEXT)
    Lokalisation = Column(TEXT)
    AUs = relationship("ArbeitsunfaehigkeitsBescheinigung",
        secondary=association_table, back_populates="Krankheitsursachen"
    )


    def __init__(self, values: dict):
        assert 'IcdCode' in values, "IcdCode not in message"
        self.IcdCode = values['IcdCode']
        if 'Qualifikation' in values:
            self.Qualifikation = values['Qualifikation']
        if 'Lokalisation' in values:
            self.Lokalisation = values['Lokalisation']

logger.info("Connecting to database")
engine = create_engine(os.getenv('DATABASE_URL'))
Base.metadata.create_all(engine)
# connection = engine.connect()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

logger.info("Setting up Kafka consumer")
from confluent_kafka import Consumer

conf = {'bootstrap.servers': os.getenv('KAFKA_BROKERS'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

from confluent_kafka import KafkaError, KafkaException
import sys
import json

def msg_process(msg):
    value = msg.value().decode('utf-8')
    logger.info('%% %s [%d] at offset %d with key %s:\n' %
          (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    logger.info(value)

    
    # find out if message is eAU
    try:
        value = json.loads(value)
        if '_KafkaEventVersion' in value and 'ProzessBasisinformation' in value and 'ProzessPersonenInformation' in value and 'ArbeitsunfaehigkeitsBescheinigung' in value:
            eau = eAU(msg.offset(), value)
            session.add(eau)
            # prozessBasisInformation
            prozess_basis_info = ProzessBasisinformation(value['ProzessBasisinformation'])
            session.add(prozess_basis_info)
            eau.ProzessBasisinformation = prozess_basis_info
            # ProzessPersonenInformation
            prozess_personen_info = ProzessPersonenInformation(value['ProzessPersonenInformation'])
            session.add(prozess_personen_info)
            eau.ProzessPersonenInformation = prozess_personen_info
            # IcdCodes
            krankheitsursachen = []
            for icd in value['ArbeitsunfaehigkeitsBescheinigung']['Krankheitsursache']:
                krankheitsursache = Krankheitsursache(icd)
                session.add(krankheitsursache)
                krankheitsursachen.append(krankheitsursache)
            # ArbeitsunfaehigkeitsBescheinigung
            arbeitsunfaehigkeits_bescheinigung = ArbeitsunfaehigkeitsBescheinigung(value['ArbeitsunfaehigkeitsBescheinigung'])
            session.add(arbeitsunfaehigkeits_bescheinigung)
            arbeitsunfaehigkeits_bescheinigung.Krankheitsursachen = krankheitsursachen
            
            eAU.ArbeitsunfaehigkeitsBescheinigung = arbeitsunfaehigkeits_bescheinigung

            session.commit()

    except AssertionError as e:
        logger.error("Assertion Error: " + str(e))    

    except Exception as e:
        logger.error("Other Error: " + str(e))
        data = Data(
            key=str(msg.key().decode('utf-8')) if msg.key() else None,
            offset=msg.offset(),
            topic=msg.topic(),
            value=json.dumps(msg.value().decode('utf-8'))
            )
        session.add(data)
        session.commit()
        if msg.headers() : 
            for h in msg.headers():
                head = Headers(
                    key = h[0],
                    value = h[1].decode('utf-8')
                )
                head.data = data
                session.add(head)
            session.commit()

running = True

def basic_consume_loop(consumer, topics):
    try:
        logger.info("Subscribing to topics: {}".format(topics))
        consumer.subscribe(topics)

        while running:
            logger.debug("polling")
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.debug('Received message: {}'.format(msg))
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False
    
if __name__ == '__main__':
    basic_consume_loop(consumer, [os.getenv('KAFKA_TOPIC')])