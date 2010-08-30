// Run w/, e.g.: $ ringo test/all

var assert = require('assert');
var dbPath = module.resolve('db');
var {Store} = require('ringo/storage/berkeleystore');
var store = new Store(dbPath);
var personId, person;
var Person = store.defineEntity('Person');
const FIRST_NAME_1 = 'Hans';
const FIRST_NAME_2 = 'Herbert';
const LAST_NAME = 'Wurst';
const BIRTH_DATE_MILLIS = 123456789000;
const BIRTH_YEAR = new Date(BIRTH_DATE_MILLIS).getFullYear();
const SSN_1 = 'AT-1234291173';
const SSN_2 = 'AT-4321291173';
const VITAE = 'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, ' +
        'sed diam nonumy eirmod tempor invidunt ut labore et dolore magna ' +
        'aliquyam erat, sed diam voluptua. At vero eos et accusam et justo ' +
        'duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata ' +
        'sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, ' +
        'consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ' +
        'ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero ' +
        'eos et accusam et justo duo dolores et ea rebum. Stet clita kasd ' +
        'gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.';

exports.setUp = exports.tearDown = function () {
    for each (let instance in Person.all()) {
        instance.remove(); // Clean up.
    }
};

exports.testPersistCreation = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    assert.equal(FIRST_NAME_1, person.firstName);
    assert.equal(LAST_NAME, person.lastName);
    assert.deepEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    assert.equal(BIRTH_YEAR, person.birthYear);
    assert.equal(VITAE, person.vitae);
};

exports.testPersistUpdating = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    personId = person._id;
    person.firstName = FIRST_NAME_2;
    person.save();
    person = Person.get(personId);
    assertPerson();
    assert.equal(FIRST_NAME_2, person.firstName);
    assert.equal(LAST_NAME, person.lastName);
    assert.deepEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    assert.equal(BIRTH_YEAR, person.birthYear);
    assert.equal(VITAE, person.vitae);
};

exports.testPersistDeletion = function () {
    person = createTestPerson();
    person.save();
    person = Person.all()[0];
    assertPerson();
    personId = person._id;
    person.remove();
    person = Person.get(personId);
    assert.isNull(person);
    assert.equal(0, Person.all().length);
};

exports.testBasicQuerying = function () {
    person = createTestPerson();
    person.save();
    person = createTestPerson();
    person.firstName = FIRST_NAME_2;
    person.ssn = SSN_2;
    person.save();
    assert.isTrue(Person.all()[0] instanceof Storable &&
            Person.all()[0] instanceof Person);
    assert.equal(2, Person.all().length);
    assert.equal(LAST_NAME, Person.all()[0].lastName);
    var queriedPerson = Person.query().equals('firstName', FIRST_NAME_1).
            select()[0];
    assert.isTrue(queriedPerson instanceof Storable &&
            queriedPerson instanceof Person);
    assert.equal(1, Person.query().equals('firstName', FIRST_NAME_1).select().
            length);
    assert.equal(FIRST_NAME_1, Person.query().equals('firstName', FIRST_NAME_1).
            select('firstName')[0]);
    assert.equal(2, Person.query().equals('lastName', LAST_NAME).select().
            length);
    assert.equal(SSN_2, Person.query().equals('lastName', LAST_NAME).
            equals('firstName', FIRST_NAME_2).select('ssn')[0]);
    testGreaterLessQuerying();
};

function testGreaterLessQuerying() {
    assert.equal(2, Person.query().greater('birthYear', BIRTH_YEAR - 1).select().
            length);
    assert.equal(0, Person.query().greater('birthYear', BIRTH_YEAR + 1).select().
            length);
    assert.equal(2, Person.query().less('birthYear', BIRTH_YEAR + 1).select().
            length);
    assert.equal(0, Person.query().less('birthYear', BIRTH_YEAR - 1).select().
            length);
    assert.equal(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR).
            select().length);
    assert.equal(2, Person.query().greaterEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    assert.equal(0, Person.query().greaterEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    assert.equal(2, Person.query().lessEquals('birthYear', BIRTH_YEAR).select().
            length);
    assert.equal(2, Person.query().lessEquals('birthYear', BIRTH_YEAR + 1).
            select().length);
    assert.equal(0, Person.query().lessEquals('birthYear', BIRTH_YEAR - 1).
            select().length);
    assert.equal(2, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assert.equal(0, Person.query().greater('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assert.equal(2, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS +
            1000)).select().length);
    assert.equal(0, Person.query().less('birthDate', new Date(BIRTH_DATE_MILLIS)
            ).select().length);
    assert.equal(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assert.equal(2, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assert.equal(0, Person.query().greaterEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    assert.equal(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS)).select().length);
    assert.equal(2, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS + 1000)).select().length);
    assert.equal(0, Person.query().lessEquals('birthDate', new Date(
            BIRTH_DATE_MILLIS - 1000)).select().length);
    assert.equal(LAST_NAME, Person.query().equals('lastName', LAST_NAME).
            greater('birthDate', new Date(BIRTH_DATE_MILLIS - 1000)).
            less('birthYear', BIRTH_YEAR + 1).select('lastName')[0]);
}

function createTestPerson() {
    return new Person({firstName: FIRST_NAME_1, lastName: LAST_NAME,
            birthDate: new Date(BIRTH_DATE_MILLIS), birthYear: BIRTH_YEAR,
            ssn: SSN_1, vitae: VITAE});
}

function assertPerson() {
    assert.isNotNull(person);
    assert.isTrue(person instanceof Storable &&
            person instanceof Person);
}

if (require.main == module.id) {
    require('test').run(exports);
}

