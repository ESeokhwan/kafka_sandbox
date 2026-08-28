package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.globalsequence.generated.CoordinatorRecordType;

public class GlobalSequenceCoordinatorRecordSerde extends CoordinatorRecordSerde {
    @Override
    protected ApiMessage apiMessageKeyFor(short recordType) {
        try {
            return CoordinatorRecordType.fromId(recordType).newRecordKey();
        } catch (UnsupportedVersionException ex) {
            throw new UnknownRecordTypeException(recordType);
        }
    }

    @Override
    protected ApiMessage apiMessageValueFor(short recordType) {
        try {
            return CoordinatorRecordType.fromId(recordType).newRecordValue();
        } catch (UnsupportedVersionException ex) {
            throw new UnknownRecordTypeException(recordType);
        }
    }
}