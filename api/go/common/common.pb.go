// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.1
// 	protoc        v4.24.4
// source: common/common.proto

package common

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Modulation int32

const (
	// LoRa
	Modulation_LORA Modulation = 0
	// FSK
	Modulation_FSK Modulation = 1
	// LR-FHSS
	Modulation_LR_FHSS Modulation = 2
)

// Enum value maps for Modulation.
var (
	Modulation_name = map[int32]string{
		0: "LORA",
		1: "FSK",
		2: "LR_FHSS",
	}
	Modulation_value = map[string]int32{
		"LORA":    0,
		"FSK":     1,
		"LR_FHSS": 2,
	}
)

func (x Modulation) Enum() *Modulation {
	p := new(Modulation)
	*p = x
	return p
}

func (x Modulation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Modulation) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[0].Descriptor()
}

func (Modulation) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[0]
}

func (x Modulation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Modulation.Descriptor instead.
func (Modulation) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{0}
}

type Region int32

const (
	// EU868
	Region_EU868 Region = 0
	// US915
	Region_US915 Region = 2
	// CN779
	Region_CN779 Region = 3
	// EU433
	Region_EU433 Region = 4
	// AU915
	Region_AU915 Region = 5
	// CN470
	Region_CN470 Region = 6
	// AS923
	Region_AS923 Region = 7
	// AS923 with -1.80 MHz frequency offset
	Region_AS923_2 Region = 12
	// AS923 with -6.60 MHz frequency offset
	Region_AS923_3 Region = 13
	// (AS923 with -5.90 MHz frequency offset).
	Region_AS923_4 Region = 14
	// KR920
	Region_KR920 Region = 8
	// IN865
	Region_IN865 Region = 9
	// RU864
	Region_RU864 Region = 10
	// ISM2400 (LoRaWAN 2.4 GHz)
	Region_ISM2400 Region = 11
)

// Enum value maps for Region.
var (
	Region_name = map[int32]string{
		0:  "EU868",
		2:  "US915",
		3:  "CN779",
		4:  "EU433",
		5:  "AU915",
		6:  "CN470",
		7:  "AS923",
		12: "AS923_2",
		13: "AS923_3",
		14: "AS923_4",
		8:  "KR920",
		9:  "IN865",
		10: "RU864",
		11: "ISM2400",
	}
	Region_value = map[string]int32{
		"EU868":   0,
		"US915":   2,
		"CN779":   3,
		"EU433":   4,
		"AU915":   5,
		"CN470":   6,
		"AS923":   7,
		"AS923_2": 12,
		"AS923_3": 13,
		"AS923_4": 14,
		"KR920":   8,
		"IN865":   9,
		"RU864":   10,
		"ISM2400": 11,
	}
)

func (x Region) Enum() *Region {
	p := new(Region)
	*p = x
	return p
}

func (x Region) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Region) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[1].Descriptor()
}

func (Region) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[1]
}

func (x Region) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Region.Descriptor instead.
func (Region) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{1}
}

type MType int32

const (
	// JoinRequest.
	MType_JOIN_REQUEST MType = 0
	// JoinAccept.
	MType_JOIN_ACCEPT MType = 1
	// UnconfirmedDataUp.
	MType_UNCONFIRMED_DATA_UP MType = 2
	// UnconfirmedDataDown.
	MType_UNCONFIRMED_DATA_DOWN MType = 3
	// ConfirmedDataUp.
	MType_CONFIRMED_DATA_UP MType = 4
	// ConfirmedDataDown.
	MType_CONFIRMED_DATA_DOWN MType = 5
	// RejoinRequest.
	MType_REJOIN_REQUEST MType = 6
	// Proprietary.
	MType_PROPRIETARY MType = 7
)

// Enum value maps for MType.
var (
	MType_name = map[int32]string{
		0: "JOIN_REQUEST",
		1: "JOIN_ACCEPT",
		2: "UNCONFIRMED_DATA_UP",
		3: "UNCONFIRMED_DATA_DOWN",
		4: "CONFIRMED_DATA_UP",
		5: "CONFIRMED_DATA_DOWN",
		6: "REJOIN_REQUEST",
		7: "PROPRIETARY",
	}
	MType_value = map[string]int32{
		"JOIN_REQUEST":          0,
		"JOIN_ACCEPT":           1,
		"UNCONFIRMED_DATA_UP":   2,
		"UNCONFIRMED_DATA_DOWN": 3,
		"CONFIRMED_DATA_UP":     4,
		"CONFIRMED_DATA_DOWN":   5,
		"REJOIN_REQUEST":        6,
		"PROPRIETARY":           7,
	}
)

func (x MType) Enum() *MType {
	p := new(MType)
	*p = x
	return p
}

func (x MType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MType) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[2].Descriptor()
}

func (MType) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[2]
}

func (x MType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MType.Descriptor instead.
func (MType) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{2}
}

type MacVersion int32

const (
	MacVersion_LORAWAN_1_0_0 MacVersion = 0
	MacVersion_LORAWAN_1_0_1 MacVersion = 1
	MacVersion_LORAWAN_1_0_2 MacVersion = 2
	MacVersion_LORAWAN_1_0_3 MacVersion = 3
	MacVersion_LORAWAN_1_0_4 MacVersion = 4
	MacVersion_LORAWAN_1_1_0 MacVersion = 5
)

// Enum value maps for MacVersion.
var (
	MacVersion_name = map[int32]string{
		0: "LORAWAN_1_0_0",
		1: "LORAWAN_1_0_1",
		2: "LORAWAN_1_0_2",
		3: "LORAWAN_1_0_3",
		4: "LORAWAN_1_0_4",
		5: "LORAWAN_1_1_0",
	}
	MacVersion_value = map[string]int32{
		"LORAWAN_1_0_0": 0,
		"LORAWAN_1_0_1": 1,
		"LORAWAN_1_0_2": 2,
		"LORAWAN_1_0_3": 3,
		"LORAWAN_1_0_4": 4,
		"LORAWAN_1_1_0": 5,
	}
)

func (x MacVersion) Enum() *MacVersion {
	p := new(MacVersion)
	*p = x
	return p
}

func (x MacVersion) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MacVersion) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[3].Descriptor()
}

func (MacVersion) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[3]
}

func (x MacVersion) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MacVersion.Descriptor instead.
func (MacVersion) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{3}
}

type RegParamsRevision int32

const (
	RegParamsRevision_A           RegParamsRevision = 0
	RegParamsRevision_B           RegParamsRevision = 1
	RegParamsRevision_RP002_1_0_0 RegParamsRevision = 2
	RegParamsRevision_RP002_1_0_1 RegParamsRevision = 3
	RegParamsRevision_RP002_1_0_2 RegParamsRevision = 4
	RegParamsRevision_RP002_1_0_3 RegParamsRevision = 5
	RegParamsRevision_RP002_1_0_4 RegParamsRevision = 6
)

// Enum value maps for RegParamsRevision.
var (
	RegParamsRevision_name = map[int32]string{
		0: "A",
		1: "B",
		2: "RP002_1_0_0",
		3: "RP002_1_0_1",
		4: "RP002_1_0_2",
		5: "RP002_1_0_3",
		6: "RP002_1_0_4",
	}
	RegParamsRevision_value = map[string]int32{
		"A":           0,
		"B":           1,
		"RP002_1_0_0": 2,
		"RP002_1_0_1": 3,
		"RP002_1_0_2": 4,
		"RP002_1_0_3": 5,
		"RP002_1_0_4": 6,
	}
)

func (x RegParamsRevision) Enum() *RegParamsRevision {
	p := new(RegParamsRevision)
	*p = x
	return p
}

func (x RegParamsRevision) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RegParamsRevision) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[4].Descriptor()
}

func (RegParamsRevision) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[4]
}

func (x RegParamsRevision) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RegParamsRevision.Descriptor instead.
func (RegParamsRevision) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{4}
}

type LocationSource int32

const (
	// Unknown.
	LocationSource_UNKNOWN LocationSource = 0
	// GPS.
	LocationSource_GPS LocationSource = 1
	// Manually configured.
	LocationSource_CONFIG LocationSource = 2
	// Geo resolver (TDOA).
	LocationSource_GEO_RESOLVER_TDOA LocationSource = 3
	// Geo resolver (RSSI).
	LocationSource_GEO_RESOLVER_RSSI LocationSource = 4
	// Geo resolver (GNSS).
	LocationSource_GEO_RESOLVER_GNSS LocationSource = 5
	// Geo resolver (WIFI).
	LocationSource_GEO_RESOLVER_WIFI LocationSource = 6
)

// Enum value maps for LocationSource.
var (
	LocationSource_name = map[int32]string{
		0: "UNKNOWN",
		1: "GPS",
		2: "CONFIG",
		3: "GEO_RESOLVER_TDOA",
		4: "GEO_RESOLVER_RSSI",
		5: "GEO_RESOLVER_GNSS",
		6: "GEO_RESOLVER_WIFI",
	}
	LocationSource_value = map[string]int32{
		"UNKNOWN":           0,
		"GPS":               1,
		"CONFIG":            2,
		"GEO_RESOLVER_TDOA": 3,
		"GEO_RESOLVER_RSSI": 4,
		"GEO_RESOLVER_GNSS": 5,
		"GEO_RESOLVER_WIFI": 6,
	}
)

func (x LocationSource) Enum() *LocationSource {
	p := new(LocationSource)
	*p = x
	return p
}

func (x LocationSource) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (LocationSource) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[5].Descriptor()
}

func (LocationSource) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[5]
}

func (x LocationSource) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use LocationSource.Descriptor instead.
func (LocationSource) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{5}
}

type Aggregation int32

const (
	// Hour.
	Aggregation_HOUR Aggregation = 0
	// Day.
	Aggregation_DAY Aggregation = 1
	// Month.
	Aggregation_MONTH Aggregation = 2
	// Minute.
	Aggregation_MINUTE Aggregation = 3
)

// Enum value maps for Aggregation.
var (
	Aggregation_name = map[int32]string{
		0: "HOUR",
		1: "DAY",
		2: "MONTH",
		3: "MINUTE",
	}
	Aggregation_value = map[string]int32{
		"HOUR":   0,
		"DAY":    1,
		"MONTH":  2,
		"MINUTE": 3,
	}
)

func (x Aggregation) Enum() *Aggregation {
	p := new(Aggregation)
	*p = x
	return p
}

func (x Aggregation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Aggregation) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[6].Descriptor()
}

func (Aggregation) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[6]
}

func (x Aggregation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Aggregation.Descriptor instead.
func (Aggregation) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{6}
}

type MetricKind int32

const (
	// Incrementing counters that never decrease (these are not reset on each
	// reading).
	MetricKind_COUNTER MetricKind = 0
	// Counters that do get reset upon reading.
	MetricKind_ABSOLUTE MetricKind = 1
	// E.g. a temperature value.
	MetricKind_GAUGE MetricKind = 2
)

// Enum value maps for MetricKind.
var (
	MetricKind_name = map[int32]string{
		0: "COUNTER",
		1: "ABSOLUTE",
		2: "GAUGE",
	}
	MetricKind_value = map[string]int32{
		"COUNTER":  0,
		"ABSOLUTE": 1,
		"GAUGE":    2,
	}
)

func (x MetricKind) Enum() *MetricKind {
	p := new(MetricKind)
	*p = x
	return p
}

func (x MetricKind) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MetricKind) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[7].Descriptor()
}

func (MetricKind) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[7]
}

func (x MetricKind) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MetricKind.Descriptor instead.
func (MetricKind) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{7}
}

type Regulation int32

const (
	// Unknown.
	Regulation_REGULATION_UNKNOWN Regulation = 0
	// ETSI EN 300 220.
	Regulation_ETSI_EN_300_220 Regulation = 1
)

// Enum value maps for Regulation.
var (
	Regulation_name = map[int32]string{
		0: "REGULATION_UNKNOWN",
		1: "ETSI_EN_300_220",
	}
	Regulation_value = map[string]int32{
		"REGULATION_UNKNOWN": 0,
		"ETSI_EN_300_220":    1,
	}
)

func (x Regulation) Enum() *Regulation {
	p := new(Regulation)
	*p = x
	return p
}

func (x Regulation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Regulation) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[8].Descriptor()
}

func (Regulation) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[8]
}

func (x Regulation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Regulation.Descriptor instead.
func (Regulation) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{8}
}

type DeviceClass int32

const (
	// Class-A.
	DeviceClass_CLASS_A DeviceClass = 0
	// Class-B.
	DeviceClass_CLASS_B DeviceClass = 1
	// Class-C.
	DeviceClass_CLASS_C DeviceClass = 2
)

// Enum value maps for DeviceClass.
var (
	DeviceClass_name = map[int32]string{
		0: "CLASS_A",
		1: "CLASS_B",
		2: "CLASS_C",
	}
	DeviceClass_value = map[string]int32{
		"CLASS_A": 0,
		"CLASS_B": 1,
		"CLASS_C": 2,
	}
)

func (x DeviceClass) Enum() *DeviceClass {
	p := new(DeviceClass)
	*p = x
	return p
}

func (x DeviceClass) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DeviceClass) Descriptor() protoreflect.EnumDescriptor {
	return file_common_common_proto_enumTypes[9].Descriptor()
}

func (DeviceClass) Type() protoreflect.EnumType {
	return &file_common_common_proto_enumTypes[9]
}

func (x DeviceClass) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DeviceClass.Descriptor instead.
func (DeviceClass) EnumDescriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{9}
}

type Location struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Latitude.
	Latitude float64 `protobuf:"fixed64,1,opt,name=latitude,proto3" json:"latitude,omitempty"`
	// Longitude.
	Longitude float64 `protobuf:"fixed64,2,opt,name=longitude,proto3" json:"longitude,omitempty"`
	// Altitude.
	Altitude float64 `protobuf:"fixed64,3,opt,name=altitude,proto3" json:"altitude,omitempty"`
	// Location source.
	Source LocationSource `protobuf:"varint,4,opt,name=source,proto3,enum=common.LocationSource" json:"source,omitempty"`
	// Accuracy.
	Accuracy float32 `protobuf:"fixed32,5,opt,name=accuracy,proto3" json:"accuracy,omitempty"`
}

func (x *Location) Reset() {
	*x = Location{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_common_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Location) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Location) ProtoMessage() {}

func (x *Location) ProtoReflect() protoreflect.Message {
	mi := &file_common_common_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Location.ProtoReflect.Descriptor instead.
func (*Location) Descriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{0}
}

func (x *Location) GetLatitude() float64 {
	if x != nil {
		return x.Latitude
	}
	return 0
}

func (x *Location) GetLongitude() float64 {
	if x != nil {
		return x.Longitude
	}
	return 0
}

func (x *Location) GetAltitude() float64 {
	if x != nil {
		return x.Altitude
	}
	return 0
}

func (x *Location) GetSource() LocationSource {
	if x != nil {
		return x.Source
	}
	return LocationSource_UNKNOWN
}

func (x *Location) GetAccuracy() float32 {
	if x != nil {
		return x.Accuracy
	}
	return 0
}

type KeyEnvelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// KEK label.
	KekLabel string `protobuf:"bytes,1,opt,name=kek_label,json=kekLabel,proto3" json:"kek_label,omitempty"`
	// AES key (when the kek_label is set, this value must first be decrypted).
	AesKey []byte `protobuf:"bytes,2,opt,name=aes_key,json=aesKey,proto3" json:"aes_key,omitempty"`
}

func (x *KeyEnvelope) Reset() {
	*x = KeyEnvelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_common_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyEnvelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyEnvelope) ProtoMessage() {}

func (x *KeyEnvelope) ProtoReflect() protoreflect.Message {
	mi := &file_common_common_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyEnvelope.ProtoReflect.Descriptor instead.
func (*KeyEnvelope) Descriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{1}
}

func (x *KeyEnvelope) GetKekLabel() string {
	if x != nil {
		return x.KekLabel
	}
	return ""
}

func (x *KeyEnvelope) GetAesKey() []byte {
	if x != nil {
		return x.AesKey
	}
	return nil
}

type Metric struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Name.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Timestamps.
	Timestamps []*timestamppb.Timestamp `protobuf:"bytes,2,rep,name=timestamps,proto3" json:"timestamps,omitempty"`
	// Datasets.
	Datasets []*MetricDataset `protobuf:"bytes,3,rep,name=datasets,proto3" json:"datasets,omitempty"`
	// Kind.
	Kind MetricKind `protobuf:"varint,4,opt,name=kind,proto3,enum=common.MetricKind" json:"kind,omitempty"`
}

func (x *Metric) Reset() {
	*x = Metric{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_common_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Metric) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Metric) ProtoMessage() {}

func (x *Metric) ProtoReflect() protoreflect.Message {
	mi := &file_common_common_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Metric.ProtoReflect.Descriptor instead.
func (*Metric) Descriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{2}
}

func (x *Metric) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Metric) GetTimestamps() []*timestamppb.Timestamp {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *Metric) GetDatasets() []*MetricDataset {
	if x != nil {
		return x.Datasets
	}
	return nil
}

func (x *Metric) GetKind() MetricKind {
	if x != nil {
		return x.Kind
	}
	return MetricKind_COUNTER
}

type MetricDataset struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Label.
	Label string `protobuf:"bytes,1,opt,name=label,proto3" json:"label,omitempty"`
	// Data.
	// Each value index corresponds with the same timestamp index of the Metric.
	Data []float32 `protobuf:"fixed32,2,rep,packed,name=data,proto3" json:"data,omitempty"`
}

func (x *MetricDataset) Reset() {
	*x = MetricDataset{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_common_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MetricDataset) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MetricDataset) ProtoMessage() {}

func (x *MetricDataset) ProtoReflect() protoreflect.Message {
	mi := &file_common_common_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MetricDataset.ProtoReflect.Descriptor instead.
func (*MetricDataset) Descriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{3}
}

func (x *MetricDataset) GetLabel() string {
	if x != nil {
		return x.Label
	}
	return ""
}

func (x *MetricDataset) GetData() []float32 {
	if x != nil {
		return x.Data
	}
	return nil
}

// Join-Server context.
type JoinServerContext struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Session-key ID.
	SessionKeyId string `protobuf:"bytes,1,opt,name=session_key_id,json=sessionKeyId,proto3" json:"session_key_id,omitempty"`
	// AppSKey envelope.
	AppSKey *KeyEnvelope `protobuf:"bytes,2,opt,name=app_s_key,json=appSKey,proto3" json:"app_s_key,omitempty"`
}

func (x *JoinServerContext) Reset() {
	*x = JoinServerContext{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_common_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JoinServerContext) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JoinServerContext) ProtoMessage() {}

func (x *JoinServerContext) ProtoReflect() protoreflect.Message {
	mi := &file_common_common_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JoinServerContext.ProtoReflect.Descriptor instead.
func (*JoinServerContext) Descriptor() ([]byte, []int) {
	return file_common_common_proto_rawDescGZIP(), []int{4}
}

func (x *JoinServerContext) GetSessionKeyId() string {
	if x != nil {
		return x.SessionKeyId
	}
	return ""
}

func (x *JoinServerContext) GetAppSKey() *KeyEnvelope {
	if x != nil {
		return x.AppSKey
	}
	return nil
}

var File_common_common_proto protoreflect.FileDescriptor

var file_common_common_proto_rawDesc = []byte{
	0x0a, 0x13, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x1a, 0x1f, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xac,
	0x01, 0x0a, 0x08, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x6c,
	0x61, 0x74, 0x69, 0x74, 0x75, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x01, 0x52, 0x08, 0x6c,
	0x61, 0x74, 0x69, 0x74, 0x75, 0x64, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x6c, 0x6f, 0x6e, 0x67, 0x69,
	0x74, 0x75, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x52, 0x09, 0x6c, 0x6f, 0x6e, 0x67,
	0x69, 0x74, 0x75, 0x64, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x61, 0x6c, 0x74, 0x69, 0x74, 0x75, 0x64,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x01, 0x52, 0x08, 0x61, 0x6c, 0x74, 0x69, 0x74, 0x75, 0x64,
	0x65, 0x12, 0x2e, 0x0a, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x16, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x4c, 0x6f, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x06, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x61, 0x63, 0x63, 0x75, 0x72, 0x61, 0x63, 0x79, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x02, 0x52, 0x08, 0x61, 0x63, 0x63, 0x75, 0x72, 0x61, 0x63, 0x79, 0x22, 0x43, 0x0a,
	0x0b, 0x4b, 0x65, 0x79, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x1b, 0x0a, 0x09,
	0x6b, 0x65, 0x6b, 0x5f, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x08, 0x6b, 0x65, 0x6b, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x12, 0x17, 0x0a, 0x07, 0x61, 0x65, 0x73,
	0x5f, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x61, 0x65, 0x73, 0x4b,
	0x65, 0x79, 0x22, 0xb3, 0x01, 0x0a, 0x06, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x12, 0x12, 0x0a,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x3a, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x12, 0x31, 0x0a,
	0x08, 0x64, 0x61, 0x74, 0x61, 0x73, 0x65, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x15, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x44,
	0x61, 0x74, 0x61, 0x73, 0x65, 0x74, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x73, 0x65, 0x74, 0x73,
	0x12, 0x26, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x4b, 0x69,
	0x6e, 0x64, 0x52, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x22, 0x39, 0x0a, 0x0d, 0x4d, 0x65, 0x74, 0x72,
	0x69, 0x63, 0x44, 0x61, 0x74, 0x61, 0x73, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x6c, 0x61, 0x62,
	0x65, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x03, 0x28, 0x02, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x22, 0x6a, 0x0a, 0x11, 0x4a, 0x6f, 0x69, 0x6e, 0x53, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x12, 0x24, 0x0a, 0x0e, 0x73, 0x65, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x5f, 0x6b, 0x65, 0x79, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0c, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x4b, 0x65, 0x79, 0x49, 0x64, 0x12, 0x2f,
	0x0a, 0x09, 0x61, 0x70, 0x70, 0x5f, 0x73, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x13, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x4b, 0x65, 0x79, 0x45, 0x6e,
	0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x52, 0x07, 0x61, 0x70, 0x70, 0x53, 0x4b, 0x65, 0x79, 0x2a,
	0x2c, 0x0a, 0x0a, 0x4d, 0x6f, 0x64, 0x75, 0x6c, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x08, 0x0a,
	0x04, 0x4c, 0x4f, 0x52, 0x41, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x46, 0x53, 0x4b, 0x10, 0x01,
	0x12, 0x0b, 0x0a, 0x07, 0x4c, 0x52, 0x5f, 0x46, 0x48, 0x53, 0x53, 0x10, 0x02, 0x2a, 0xaa, 0x01,
	0x0a, 0x06, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x55, 0x38, 0x36,
	0x38, 0x10, 0x00, 0x12, 0x09, 0x0a, 0x05, 0x55, 0x53, 0x39, 0x31, 0x35, 0x10, 0x02, 0x12, 0x09,
	0x0a, 0x05, 0x43, 0x4e, 0x37, 0x37, 0x39, 0x10, 0x03, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x55, 0x34,
	0x33, 0x33, 0x10, 0x04, 0x12, 0x09, 0x0a, 0x05, 0x41, 0x55, 0x39, 0x31, 0x35, 0x10, 0x05, 0x12,
	0x09, 0x0a, 0x05, 0x43, 0x4e, 0x34, 0x37, 0x30, 0x10, 0x06, 0x12, 0x09, 0x0a, 0x05, 0x41, 0x53,
	0x39, 0x32, 0x33, 0x10, 0x07, 0x12, 0x0b, 0x0a, 0x07, 0x41, 0x53, 0x39, 0x32, 0x33, 0x5f, 0x32,
	0x10, 0x0c, 0x12, 0x0b, 0x0a, 0x07, 0x41, 0x53, 0x39, 0x32, 0x33, 0x5f, 0x33, 0x10, 0x0d, 0x12,
	0x0b, 0x0a, 0x07, 0x41, 0x53, 0x39, 0x32, 0x33, 0x5f, 0x34, 0x10, 0x0e, 0x12, 0x09, 0x0a, 0x05,
	0x4b, 0x52, 0x39, 0x32, 0x30, 0x10, 0x08, 0x12, 0x09, 0x0a, 0x05, 0x49, 0x4e, 0x38, 0x36, 0x35,
	0x10, 0x09, 0x12, 0x09, 0x0a, 0x05, 0x52, 0x55, 0x38, 0x36, 0x34, 0x10, 0x0a, 0x12, 0x0b, 0x0a,
	0x07, 0x49, 0x53, 0x4d, 0x32, 0x34, 0x30, 0x30, 0x10, 0x0b, 0x2a, 0xb3, 0x01, 0x0a, 0x05, 0x4d,
	0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x0c, 0x4a, 0x4f, 0x49, 0x4e, 0x5f, 0x52, 0x45, 0x51,
	0x55, 0x45, 0x53, 0x54, 0x10, 0x00, 0x12, 0x0f, 0x0a, 0x0b, 0x4a, 0x4f, 0x49, 0x4e, 0x5f, 0x41,
	0x43, 0x43, 0x45, 0x50, 0x54, 0x10, 0x01, 0x12, 0x17, 0x0a, 0x13, 0x55, 0x4e, 0x43, 0x4f, 0x4e,
	0x46, 0x49, 0x52, 0x4d, 0x45, 0x44, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x5f, 0x55, 0x50, 0x10, 0x02,
	0x12, 0x19, 0x0a, 0x15, 0x55, 0x4e, 0x43, 0x4f, 0x4e, 0x46, 0x49, 0x52, 0x4d, 0x45, 0x44, 0x5f,
	0x44, 0x41, 0x54, 0x41, 0x5f, 0x44, 0x4f, 0x57, 0x4e, 0x10, 0x03, 0x12, 0x15, 0x0a, 0x11, 0x43,
	0x4f, 0x4e, 0x46, 0x49, 0x52, 0x4d, 0x45, 0x44, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x5f, 0x55, 0x50,
	0x10, 0x04, 0x12, 0x17, 0x0a, 0x13, 0x43, 0x4f, 0x4e, 0x46, 0x49, 0x52, 0x4d, 0x45, 0x44, 0x5f,
	0x44, 0x41, 0x54, 0x41, 0x5f, 0x44, 0x4f, 0x57, 0x4e, 0x10, 0x05, 0x12, 0x12, 0x0a, 0x0e, 0x52,
	0x45, 0x4a, 0x4f, 0x49, 0x4e, 0x5f, 0x52, 0x45, 0x51, 0x55, 0x45, 0x53, 0x54, 0x10, 0x06, 0x12,
	0x0f, 0x0a, 0x0b, 0x50, 0x52, 0x4f, 0x50, 0x52, 0x49, 0x45, 0x54, 0x41, 0x52, 0x59, 0x10, 0x07,
	0x2a, 0x7e, 0x0a, 0x0a, 0x4d, 0x61, 0x63, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x11,
	0x0a, 0x0d, 0x4c, 0x4f, 0x52, 0x41, 0x57, 0x41, 0x4e, 0x5f, 0x31, 0x5f, 0x30, 0x5f, 0x30, 0x10,
	0x00, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f, 0x52, 0x41, 0x57, 0x41, 0x4e, 0x5f, 0x31, 0x5f, 0x30,
	0x5f, 0x31, 0x10, 0x01, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f, 0x52, 0x41, 0x57, 0x41, 0x4e, 0x5f,
	0x31, 0x5f, 0x30, 0x5f, 0x32, 0x10, 0x02, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f, 0x52, 0x41, 0x57,
	0x41, 0x4e, 0x5f, 0x31, 0x5f, 0x30, 0x5f, 0x33, 0x10, 0x03, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f,
	0x52, 0x41, 0x57, 0x41, 0x4e, 0x5f, 0x31, 0x5f, 0x30, 0x5f, 0x34, 0x10, 0x04, 0x12, 0x11, 0x0a,
	0x0d, 0x4c, 0x4f, 0x52, 0x41, 0x57, 0x41, 0x4e, 0x5f, 0x31, 0x5f, 0x31, 0x5f, 0x30, 0x10, 0x05,
	0x2a, 0x76, 0x0a, 0x11, 0x52, 0x65, 0x67, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x52, 0x65, 0x76,
	0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x05, 0x0a, 0x01, 0x41, 0x10, 0x00, 0x12, 0x05, 0x0a, 0x01,
	0x42, 0x10, 0x01, 0x12, 0x0f, 0x0a, 0x0b, 0x52, 0x50, 0x30, 0x30, 0x32, 0x5f, 0x31, 0x5f, 0x30,
	0x5f, 0x30, 0x10, 0x02, 0x12, 0x0f, 0x0a, 0x0b, 0x52, 0x50, 0x30, 0x30, 0x32, 0x5f, 0x31, 0x5f,
	0x30, 0x5f, 0x31, 0x10, 0x03, 0x12, 0x0f, 0x0a, 0x0b, 0x52, 0x50, 0x30, 0x30, 0x32, 0x5f, 0x31,
	0x5f, 0x30, 0x5f, 0x32, 0x10, 0x04, 0x12, 0x0f, 0x0a, 0x0b, 0x52, 0x50, 0x30, 0x30, 0x32, 0x5f,
	0x31, 0x5f, 0x30, 0x5f, 0x33, 0x10, 0x05, 0x12, 0x0f, 0x0a, 0x0b, 0x52, 0x50, 0x30, 0x30, 0x32,
	0x5f, 0x31, 0x5f, 0x30, 0x5f, 0x34, 0x10, 0x06, 0x2a, 0x8e, 0x01, 0x0a, 0x0e, 0x4c, 0x6f, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x0b, 0x0a, 0x07, 0x55,
	0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x47, 0x50, 0x53, 0x10,
	0x01, 0x12, 0x0a, 0x0a, 0x06, 0x43, 0x4f, 0x4e, 0x46, 0x49, 0x47, 0x10, 0x02, 0x12, 0x15, 0x0a,
	0x11, 0x47, 0x45, 0x4f, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x4c, 0x56, 0x45, 0x52, 0x5f, 0x54, 0x44,
	0x4f, 0x41, 0x10, 0x03, 0x12, 0x15, 0x0a, 0x11, 0x47, 0x45, 0x4f, 0x5f, 0x52, 0x45, 0x53, 0x4f,
	0x4c, 0x56, 0x45, 0x52, 0x5f, 0x52, 0x53, 0x53, 0x49, 0x10, 0x04, 0x12, 0x15, 0x0a, 0x11, 0x47,
	0x45, 0x4f, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x4c, 0x56, 0x45, 0x52, 0x5f, 0x47, 0x4e, 0x53, 0x53,
	0x10, 0x05, 0x12, 0x15, 0x0a, 0x11, 0x47, 0x45, 0x4f, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x4c, 0x56,
	0x45, 0x52, 0x5f, 0x57, 0x49, 0x46, 0x49, 0x10, 0x06, 0x2a, 0x37, 0x0a, 0x0b, 0x41, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x08, 0x0a, 0x04, 0x48, 0x4f, 0x55, 0x52,
	0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x44, 0x41, 0x59, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x4d,
	0x4f, 0x4e, 0x54, 0x48, 0x10, 0x02, 0x12, 0x0a, 0x0a, 0x06, 0x4d, 0x49, 0x4e, 0x55, 0x54, 0x45,
	0x10, 0x03, 0x2a, 0x32, 0x0a, 0x0a, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x4b, 0x69, 0x6e, 0x64,
	0x12, 0x0b, 0x0a, 0x07, 0x43, 0x4f, 0x55, 0x4e, 0x54, 0x45, 0x52, 0x10, 0x00, 0x12, 0x0c, 0x0a,
	0x08, 0x41, 0x42, 0x53, 0x4f, 0x4c, 0x55, 0x54, 0x45, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x47,
	0x41, 0x55, 0x47, 0x45, 0x10, 0x02, 0x2a, 0x39, 0x0a, 0x0a, 0x52, 0x65, 0x67, 0x75, 0x6c, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x16, 0x0a, 0x12, 0x52, 0x45, 0x47, 0x55, 0x4c, 0x41, 0x54, 0x49,
	0x4f, 0x4e, 0x5f, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x13, 0x0a, 0x0f,
	0x45, 0x54, 0x53, 0x49, 0x5f, 0x45, 0x4e, 0x5f, 0x33, 0x30, 0x30, 0x5f, 0x32, 0x32, 0x30, 0x10,
	0x01, 0x2a, 0x34, 0x0a, 0x0b, 0x44, 0x65, 0x76, 0x69, 0x63, 0x65, 0x43, 0x6c, 0x61, 0x73, 0x73,
	0x12, 0x0b, 0x0a, 0x07, 0x43, 0x4c, 0x41, 0x53, 0x53, 0x5f, 0x41, 0x10, 0x00, 0x12, 0x0b, 0x0a,
	0x07, 0x43, 0x4c, 0x41, 0x53, 0x53, 0x5f, 0x42, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x43, 0x4c,
	0x41, 0x53, 0x53, 0x5f, 0x43, 0x10, 0x02, 0x42, 0x9d, 0x01, 0x0a, 0x11, 0x69, 0x6f, 0x2e, 0x63,
	0x68, 0x69, 0x72, 0x70, 0x73, 0x74, 0x61, 0x63, 0x6b, 0x2e, 0x61, 0x70, 0x69, 0x42, 0x0b, 0x43,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x31, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x68, 0x69, 0x72, 0x70, 0x73, 0x74,
	0x61, 0x63, 0x6b, 0x2f, 0x63, 0x68, 0x69, 0x72, 0x70, 0x73, 0x74, 0x61, 0x63, 0x6b, 0x2f, 0x61,
	0x70, 0x69, 0x2f, 0x67, 0x6f, 0x2f, 0x76, 0x34, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0xaa,
	0x02, 0x11, 0x43, 0x68, 0x69, 0x72, 0x70, 0x73, 0x74, 0x61, 0x63, 0x6b, 0x2e, 0x43, 0x6f, 0x6d,
	0x6d, 0x6f, 0x6e, 0xca, 0x02, 0x11, 0x43, 0x68, 0x69, 0x72, 0x70, 0x73, 0x74, 0x61, 0x63, 0x6b,
	0x5c, 0x43, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0xe2, 0x02, 0x1d, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74,
	0x61, 0x64, 0x61, 0x74, 0x61, 0x5c, 0x43, 0x68, 0x69, 0x72, 0x70, 0x73, 0x74, 0x61, 0x63, 0x6b,
	0x5c, 0x43, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_common_common_proto_rawDescOnce sync.Once
	file_common_common_proto_rawDescData = file_common_common_proto_rawDesc
)

func file_common_common_proto_rawDescGZIP() []byte {
	file_common_common_proto_rawDescOnce.Do(func() {
		file_common_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_common_common_proto_rawDescData)
	})
	return file_common_common_proto_rawDescData
}

var file_common_common_proto_enumTypes = make([]protoimpl.EnumInfo, 10)
var file_common_common_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_common_common_proto_goTypes = []interface{}{
	(Modulation)(0),               // 0: common.Modulation
	(Region)(0),                   // 1: common.Region
	(MType)(0),                    // 2: common.MType
	(MacVersion)(0),               // 3: common.MacVersion
	(RegParamsRevision)(0),        // 4: common.RegParamsRevision
	(LocationSource)(0),           // 5: common.LocationSource
	(Aggregation)(0),              // 6: common.Aggregation
	(MetricKind)(0),               // 7: common.MetricKind
	(Regulation)(0),               // 8: common.Regulation
	(DeviceClass)(0),              // 9: common.DeviceClass
	(*Location)(nil),              // 10: common.Location
	(*KeyEnvelope)(nil),           // 11: common.KeyEnvelope
	(*Metric)(nil),                // 12: common.Metric
	(*MetricDataset)(nil),         // 13: common.MetricDataset
	(*JoinServerContext)(nil),     // 14: common.JoinServerContext
	(*timestamppb.Timestamp)(nil), // 15: google.protobuf.Timestamp
}
var file_common_common_proto_depIdxs = []int32{
	5,  // 0: common.Location.source:type_name -> common.LocationSource
	15, // 1: common.Metric.timestamps:type_name -> google.protobuf.Timestamp
	13, // 2: common.Metric.datasets:type_name -> common.MetricDataset
	7,  // 3: common.Metric.kind:type_name -> common.MetricKind
	11, // 4: common.JoinServerContext.app_s_key:type_name -> common.KeyEnvelope
	5,  // [5:5] is the sub-list for method output_type
	5,  // [5:5] is the sub-list for method input_type
	5,  // [5:5] is the sub-list for extension type_name
	5,  // [5:5] is the sub-list for extension extendee
	0,  // [0:5] is the sub-list for field type_name
}

func init() { file_common_common_proto_init() }
func file_common_common_proto_init() {
	if File_common_common_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_common_common_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Location); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_common_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyEnvelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_common_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Metric); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_common_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MetricDataset); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_common_common_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JoinServerContext); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_common_common_proto_rawDesc,
			NumEnums:      10,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_common_common_proto_goTypes,
		DependencyIndexes: file_common_common_proto_depIdxs,
		EnumInfos:         file_common_common_proto_enumTypes,
		MessageInfos:      file_common_common_proto_msgTypes,
	}.Build()
	File_common_common_proto = out.File
	file_common_common_proto_rawDesc = nil
	file_common_common_proto_goTypes = nil
	file_common_common_proto_depIdxs = nil
}
