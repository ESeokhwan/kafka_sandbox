# Features for Transient Topic Assignor

## APIs


### Transient Topic Produce, Fetch

일반적인 produce API 처럼 토픽 이름을 이용해 특정 토픽으로의 produce를 진행하는 API이다. 대신 이 API 에서는 일반 토픽이 아닌 
`Transient Topic` 이라는 새로운 구조를 이용한다. `Transient Topic`은 사전에 만들어진 토픽(`__transient_topic_pool`)의 파티션을
일시적으로 할당받아서 사용하고 일정 시간동안 해당 그룹 토픽이 사용되지 않으면 할당이 해제된다. `Transient Topic`은 실시간 소통이 필요한 
일시적 그룹이 자주 만들어지고 사라지는 환경에서 효과적으로 `Kafka`를 사용할 수 있도록 지원한다.

<details>
<summary><b>Details</b></summary>

- `Transient Topic`은 단일 파티션으로만 생성 가능하고 개별 `Transient Topic` 마다 레플리케이션의 개수를 지정하는 것은 불가능하다.
- 외부에 노출된 `Transient Topic` 이름과 내부적으로 실제 할당된 토픽 파티션 정보를 관리하기 위해 `Topic Assign Map`이 사용된다.
- 새로운 `Transient Topic`의 이름으로 Produce, Fetch 요청이 들어오면 `Topic Partition Pool`에서 파티션 하나를 할당해 데이터를 해당
  topic에 저장하고 이를 `Topic Assign Map`에 저장한 뒤에 생산, 소비 로직을 수행한다. 이 과정에서 내부적으로 관리를 위한`Topic ID`가
  생성된다.
- 기존에 등록된 `Transient Topic`의 이름으로 Produce, Fetch 요청이 들어오면 `Topic Assign Map`에 저장된 정보를 바탕으로 할당된 
  파티션에 데이터를 생산 혹은 소비한다.
- 일정 시간동안 `Transient Topic`이 사용되지 않으면 자동으로 제거되고 할당된 파티션은 해제된다.
- `__transient_topic_pool`의 모든 파티션이 할당된 상태이면 기존 파티션 개수 만큼 파티션을 추가한다. (추가 될 때마다 2배씩 늘어남)
- 어떤 `Transient Topic`이 할당 해제된 이후에 또 요청이 들어오면 새로운 `Topic ID`를 가진 새로운 `Transient Topic`이 파티션을 할당받아
  생성된다.
- `Topic Assign Map`은 `__cluster_metadata` 토픽처럼 별도의 토픽(`__transient_topic_assign_map`)에 저장된다.
- `Topic Assign Map`은 일부 데이터를 메모리에 캐싱한다.
</details>

<details>
<summary><b>Implementation</b></summary>

- `Topic Assign Map`을 통해 실제 할당된 토픽 파티션의 정보를 관리하는 부분까지는 새롭게 구현을 하고 이후 로직은 기존의 Produce, Fetch
  요청 처리 로직을 활용해서 구현한다.
- `Topic Assign Map`은 `Transient Topic`의 파티션 할당 정보를 다음과 같은 형식으로 저장한다.
  ```
  {
      topic_id: UUID,
      topic_name: String,
      partition: {
          ...,
          start_offset: int
      },
      created_at: int,
      last_used_at: int,
  }
  ```
- `Topic Assign Map`을 디스크에 저장할 때에는 바로 flush를 해버린다? 바로 flush를 해버리면 `__cluster_metadata`에 metadata 저장하는
  경우와 같이 ext4에서 발생하는 문제가 여전히 발생하게 된다. 그렇다고 flush를 안 하기도 애매하다.
- `Topic Assign Map`은 현재 할당이 된 상태의 `Transient Topic`에 대한 데이터만 메모리에 캐싱해 둔다.
- `Topic Assing Map`을 캐싱해서 관리할 때에는 Hash Map 자료 구조를 이용해 토픽 이름으로 접근이 빠르도록 설계한다.
- 해제 기준에 맞게 구현한다.
</details>

<details>
<summary><b>To Do List</b></summary>

1. 데이터 모델 및 관리
   - [x] `Transient Topic`의 파티션 할당 정보를 위한 DTO Class(이하 `TransientTopicIndexDTO`) 제작
   - [x] `TransientTopicIndexDTO`를 토픽 레코드로 저장하기 위한 parser 제작
   - [ ] 인메모리 `Topic Assign Map` 캐시 제작
     - [x] `getIndexByName(groupName)`
     - [x] `createIndex(groupName, partition)`
     - [ ] `flushIndex()`
     - [x] `evictIndex(groupName)` or `evictIndex(groupId)`
   - [ ] 디스크 용 `Topic Assign Map` 제작
     - `KafkaMetadatLog.scala` 참고해서 구현
   - [x] `Topic Partition Pool`을 위한 파티션 DTO Class(이하 `TopicPartitionDTO`) 제작
     - 가능하다면 기존 `TopicPartition` 사용해도 괜찮음
   - [ ] `Topic Partition Pool` 관리 class(이하 `FreeTopicPartitionPool`) 제작
     - [ ] `allocatePartition()`
     - [ ] `releasePartition(partition)`

1. 핵심 로직 및 API 추가
   - [ ] `Topic Assign Map` interface 제작
     - [ ] `getIndexByName(groupName)`
     - [ ] `createIndex(groupName, partition)`
     - [ ] `flushIndex()`
     - [ ] (optional) `getIndexById(groupId)`: 실시간 서비스 용 x, 클라우드에서 group 별 데이터를 가져가고 싶을 때 사용
     - [ ] (optional) `getIndexesByTimestamp(timestamp)`: 무조건 디스크에서 가져오기. 실시간 서비스 용 x, 클라우드에서 group 별
       데이터를 가져가고 싶을 때 사용
   - [ ] `Transient Topic` Produce, Fetch 요청 처리 핸들러 구현

1. 생명주기 및 초기화
   - [ ] `Transient Topic`의 인덱스 정보를 저장하기 위한 초기 세팅 관련 로직 구현
     - 메모리, 디스크 `Topic Assign Map` 초기화 로직 구현
     - `__cluster_metadata` 토픽 초기화 세팅 관련 로직 참고
   - [ ] `FreeTopicPartitionPool` 초기화 로직 구현 
       - [ ] `__transient_topic` 미리 만들어 놓기
   - [ ] 추가 할당 로직 구현
   - [ ] 해제 기준 설정 및 구현

1. 기타 설정
   - [ ] 기능 관련 설정 추가
     - [ ] 임시 토픽 풀의 크기 (temporary.topic.pool.size)
     - [ ] 추가 할당 가능 여부
     - [ ] etc.
</details>

### Free Group
할당된 그룹을 강제로 해제하는 API이다.

<details>
<summary><b>Details</b></summary>

- 
</details>

<details>
<summary><b>Implementation</b></summary>

- 
</details>

<details>
<summary><b>To Do List</b></summary>

- [ ] ?
</details>

### Join Groupcast (Optionally)
Group Topic은 Groupcast 방식의 consume 기능을 제공한다. 해당 API를 이용해 특정 Temporary Topic의 데이터가 Groupcast 되는 
채널의 주소를 얻고 해당 채널을 통해 데이터를 전달 받는 방식으로 사용한다.

<details>
<summary><b>Details</b></summary>

- 
</details>

<details>
<summary><b>Implementation</b></summary>

- 
</details>

<details>
<summary><b>To Do List</b></summary>

- [ ] ?
</details>