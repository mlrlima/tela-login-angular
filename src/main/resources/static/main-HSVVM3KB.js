var _D=Object.defineProperty,bD=Object.defineProperties;var CD=Object.getOwnPropertyDescriptors;var iv=Object.getOwnPropertySymbols;var ED=Object.prototype.hasOwnProperty,wD=Object.prototype.propertyIsEnumerable;var rv=(t,n,e)=>n in t?_D(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})ED.call(n,e)&&rv(t,e,n[e]);if(iv)for(var e of iv(n))wD.call(n,e)&&rv(t,e,n[e]);return t},B=(t,n)=>bD(t,CD(n));var Ce=(t,n,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(t,n)).next())});var Pt=null,Nc=!1,mr=1,DD=null,Ue=Symbol("SIGNAL");function U(t){let n=Pt;return Pt=t,n}function kc(){return Pt}var ki={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Oi(t){if(Nc)throw new Error("");if(Pt===null)return;Pt.consumerOnSignalRead(t);let n=Pt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Pt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Pt.producers,e!==void 0&&e.producer===t)){Pt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=mr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Pt&&(!i||r.knownValidAtEpoch===mr))return;let o=bo(Pt),s={producer:t,consumer:Pt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:mr,lastReadVersion:t.version,nextConsumer:void 0};Pt.producersTail=s,n!==void 0?n.nextProducer=s:Pt.producers=s,o&&cv(t,s)}function ov(){mr++}function yr(t){if(!(bo(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===mr)){if(!t.producerMustRecompute(t)&&!_o(t)){yo(t);return}t.producerRecomputeValue(t),yo(t)}}function lf(t){if(t.consumers===void 0)return;let n=Nc;Nc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||xD(i)}}finally{Nc=n}}function df(){return Pt?.consumerAllowSignalWrites!==!1}function xD(t){t.dirty=!0,lf(t),t.consumerMarkedDirty?.(t)}function yo(t){t.dirty=!1,t.lastCleanEpoch=mr}function ci(t){return t&&sv(t),U(t)}function sv(t){if(t.producersTail?.knownValidAtEpoch===mr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Fi(t,n){U(n),t&&av(t)}function av(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(bo(t))do e=uf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function _o(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(yr(e),i!==e.version))return!0}return!1}function Pi(t){if(bo(t)){let n=t.producers;for(;n!==void 0;)n=uf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function cv(t,n){let e=t.consumersTail,i=bo(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)cv(r.producer,r)}function uf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!bo(n)){let o=n.producers;for(;o!==void 0;)o=uf(o)}return e}function bo(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function ks(t){DD?.(t)}function Os(t,n){return Object.is(t,n)}function Fs(t,n){let e=Object.create(ID);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(yr(e),Oi(e),e.value===jn)throw e.error;return e.value};return i[Ue]=e,ks(e),i}var gr=Symbol("UNSET"),vr=Symbol("COMPUTING"),jn=Symbol("ERRORED"),ID=B(C({},ki),{value:gr,dirty:!0,error:null,equal:Os,kind:"computed",producerMustRecompute(t){return t.value===gr||t.value===vr},producerRecomputeValue(t){if(t.value===vr)throw new Error("");let n=t.value;t.value=vr;let e=ci(t),i,r=!1;try{i=t.computation(),U(null),r=n!==gr&&n!==jn&&i!==jn&&t.equal(n,i)}catch(o){i=jn,t.error=o}finally{Fi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function SD(){throw new Error}var lv=SD;function dv(t){lv(t)}function ff(t){lv=t}var MD=null;function hf(t,n){let e=Object.create(Ps);e.value=t,n!==void 0&&(e.equal=n);let i=()=>uv(e);return i[Ue]=e,ks(e),[i,s=>_r(e,s),s=>Oc(e,s)]}function uv(t){return Oi(t),t.value}function _r(t,n){df()||dv(t),t.equal(t.value,n)||(t.value=n,TD(t))}function Oc(t,n){df()||dv(t),_r(t,n(t.value))}var Ps=B(C({},ki),{equal:Os,value:void 0,kind:"signal"});function TD(t){t.version++,ov(),lf(t),MD?.(t)}var pf=B(C({},ki),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function mf(t){if(t.dirty=!1,t.version>0&&!_o(t))return;t.version++;let n=ci(t);try{t.cleanup(),t.fn()}finally{Fi(t,n)}}var gf;function Fc(){return gf}function Bn(t){let n=gf;return gf=t,n}var fv=Symbol("NotFound");function Co(t){return t===fv||t?.name==="\u0275NotFound"}function vf(t,n,e){let i=Object.create(AD);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(yr(i),Oi(i),i.value===jn)throw i.error;return i.value};return o[Ue]=i,ks(i),o}function hv(t,n){yr(t),_r(t,n),yo(t)}function pv(t,n){if(yr(t),t.value===jn)throw t.error;Oc(t,n),yo(t)}var AD=B(C({},ki),{value:gr,dirty:!0,error:null,equal:Os,kind:"linkedSignal",producerMustRecompute(t){return t.value===gr||t.value===vr},producerRecomputeValue(t){if(t.value===vr)throw new Error("");let n=t.value;t.value=vr;let e=ci(t),i,r=!1;try{let o=t.source(),s=n!==gr&&n!==jn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,U(null),r=s&&i!==jn&&t.equal(n,i)}catch(o){i=jn,t.error=o}finally{Fi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function mv(t){let n=U(null);try{return t()}finally{U(n)}}function K(t){return typeof t=="function"}function Eo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Pc=Eo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function br(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ue=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(K(i))try{i()}catch(o){n=o instanceof Pc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{gv(o)}catch(s){n=n??[],s instanceof Pc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Pc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)gv(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&br(e,n)}remove(n){let{_finalizers:e}=this;e&&br(e,n),n instanceof t&&n._removeParent(this)}};ue.EMPTY=(()=>{let t=new ue;return t.closed=!0,t})();var yf=ue.EMPTY;function Lc(t){return t instanceof ue||t&&"closed"in t&&K(t.remove)&&K(t.add)&&K(t.unsubscribe)}function gv(t){K(t)?t():t.unsubscribe()}var Tn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var wo={setTimeout(t,n,...e){let{delegate:i}=wo;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=wo;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Vc(t){wo.setTimeout(()=>{let{onUnhandledError:n}=Tn;if(n)n(t);else throw t})}function Ls(){}var vv=_f("C",void 0,void 0);function yv(t){return _f("E",void 0,t)}function _v(t){return _f("N",t,void 0)}function _f(t,n,e){return{kind:t,value:n,error:e}}var Cr=null;function Do(t){if(Tn.useDeprecatedSynchronousErrorHandling){let n=!Cr;if(n&&(Cr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Cr;if(Cr=null,e)throw i}}else t()}function bv(t){Tn.useDeprecatedSynchronousErrorHandling&&Cr&&(Cr.errorThrown=!0,Cr.error=t)}var Er=class extends ue{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Lc(n)&&n.add(this)):this.destination=kD}static create(n,e,i){return new li(n,e,i)}next(n){this.isStopped?Cf(_v(n),this):this._next(n)}error(n){this.isStopped?Cf(yv(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Cf(vv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},RD=Function.prototype.bind;function bf(t,n){return RD.call(t,n)}var Ef=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){jc(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){jc(i)}else jc(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){jc(e)}}},li=class extends Er{constructor(n,e,i){super();let r;if(K(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Tn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&bf(n.next,o),error:n.error&&bf(n.error,o),complete:n.complete&&bf(n.complete,o)}):r=n}this.destination=new Ef(r)}};function jc(t){Tn.useDeprecatedSynchronousErrorHandling?bv(t):Vc(t)}function ND(t){throw t}function Cf(t,n){let{onStoppedNotification:e}=Tn;e&&wo.setTimeout(()=>e(t,n))}var kD={closed:!0,next:Ls,error:ND,complete:Ls};var xo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Kt(t){return t}function wf(...t){return Df(t)}function Df(t){return t.length===0?Kt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var te=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=FD(e)?e:new li(e,i,r);return Do(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Cv(i),new i((r,o)=>{let s=new li({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[xo](){return this}pipe(...e){return Df(e)(this)}toPromise(e){return e=Cv(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Cv(t){var n;return(n=t??Tn.Promise)!==null&&n!==void 0?n:Promise}function OD(t){return t&&K(t.next)&&K(t.error)&&K(t.complete)}function FD(t){return t&&t instanceof Er||OD(t)&&Lc(t)}function PD(t){return K(t?.lift)}function Q(t){return n=>{if(PD(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ne(t,n,e,i,r){return new xf(t,n,e,i,r)}var xf=class extends Er{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Ev=Eo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var S=(()=>{class t extends te{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Bc(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Ev}next(e){Do(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Do(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Do(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?yf:(this.currentObservers=null,o.push(e),new ue(()=>{this.currentObservers=null,br(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new te;return e.source=this,e}}return t.create=(n,e)=>new Bc(n,e),t})(),Bc=class extends S{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:yf}};var rt=class extends S{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Vs={now(){return(Vs.delegate||Date).now()},delegate:void 0};var Uc=class extends S{constructor(n=1/0,e=1/0,i=Vs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Hc=class extends ue{constructor(n,e){super()}schedule(n,e=0){return this}};var js={setInterval(t,n,...e){let{delegate:i}=js;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=js;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var zc=class extends Hc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return js.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&js.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,br(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Io=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Io.now=Vs.now;var $c=class extends Io{constructor(n,e=Io.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Bs=new $c(zc),wv=Bs;var He=new te(t=>t.complete());function Gc(t){return t&&K(t.schedule)}function If(t){return t[t.length-1]}function Wc(t){return K(If(t))?t.pop():void 0}function Un(t){return Gc(If(t))?t.pop():void 0}function Dv(t,n){return typeof If(t)=="number"?t.pop():n}function Iv(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function xv(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function wr(t){return this instanceof wr?(this.v=t,this):new wr(t)}function Sv(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(g){return function(b){return Promise.resolve(b).then(g,f)}}function a(g,b){i[g]&&(r[g]=function(x){return new Promise(function(R,ee){o.push([g,x,R,ee])>1||c(g,x)})},b&&(r[g]=b(r[g])))}function c(g,b){try{l(i[g](b))}catch(x){m(o[0][3],x)}}function l(g){g.value instanceof wr?Promise.resolve(g.value.v).then(d,f):m(o[0][2],g)}function d(g){c("next",g)}function f(g){c("throw",g)}function m(g,b){g(b),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Mv(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof xv=="function"?xv(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var qc=t=>t&&typeof t.length=="number"&&typeof t!="function";function Yc(t){return K(t?.then)}function Zc(t){return K(t[xo])}function Kc(t){return Symbol.asyncIterator&&K(t?.[Symbol.asyncIterator])}function Qc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function LD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Xc=LD();function Jc(t){return K(t?.[Xc])}function el(t){return Sv(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield wr(e.read());if(r)return yield wr(void 0);yield yield wr(i)}}finally{e.releaseLock()}})}function tl(t){return K(t?.getReader)}function Ie(t){if(t instanceof te)return t;if(t!=null){if(Zc(t))return VD(t);if(qc(t))return jD(t);if(Yc(t))return BD(t);if(Kc(t))return Tv(t);if(Jc(t))return UD(t);if(tl(t))return HD(t)}throw Qc(t)}function VD(t){return new te(n=>{let e=t[xo]();if(K(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function jD(t){return new te(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function BD(t){return new te(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Vc)})}function UD(t){return new te(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Tv(t){return new te(n=>{zD(t,n).catch(e=>n.error(e))})}function HD(t){return Tv(el(t))}function zD(t,n){var e,i,r,o;return Iv(this,void 0,void 0,function*(){try{for(e=Mv(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Gt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function nl(t,n=0){return Q((e,i)=>{e.subscribe(ne(i,r=>Gt(i,t,()=>i.next(r),n),()=>Gt(i,t,()=>i.complete(),n),r=>Gt(i,t,()=>i.error(r),n)))})}function il(t,n=0){return Q((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Av(t,n){return Ie(t).pipe(il(n),nl(n))}function Rv(t,n){return Ie(t).pipe(il(n),nl(n))}function Nv(t,n){return new te(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function kv(t,n){return new te(e=>{let i;return Gt(e,n,()=>{i=t[Xc](),Gt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>K(i?.return)&&i.return()})}function rl(t,n){if(!t)throw new Error("Iterable cannot be null");return new te(e=>{Gt(e,n,()=>{let i=t[Symbol.asyncIterator]();Gt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Ov(t,n){return rl(el(t),n)}function Fv(t,n){if(t!=null){if(Zc(t))return Av(t,n);if(qc(t))return Nv(t,n);if(Yc(t))return Rv(t,n);if(Kc(t))return rl(t,n);if(Jc(t))return kv(t,n);if(tl(t))return Ov(t,n)}throw Qc(t)}function Ne(t,n){return n?Fv(t,n):Ie(t)}function L(...t){let n=Un(t);return Ne(t,n)}function Us(t,n){let e=K(t)?t:()=>t,i=r=>r.error(e());return new te(n?r=>n.schedule(i,0,r):i)}function Hs(t){return!!t&&(t instanceof te||K(t.lift)&&K(t.subscribe))}var Dr=Eo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Pv(t){return t instanceof Date&&!isNaN(t)}function X(t,n){return Q((e,i)=>{let r=0;e.subscribe(ne(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:$D}=Array;function GD(t,n){return $D(n)?t(...n):t(n)}function ol(t){return X(n=>GD(t,n))}var{isArray:WD}=Array,{getPrototypeOf:qD,prototype:YD,keys:ZD}=Object;function sl(t){if(t.length===1){let n=t[0];if(WD(n))return{args:n,keys:null};if(KD(n)){let e=ZD(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function KD(t){return t&&typeof t=="object"&&qD(t)===YD}function al(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Sf(...t){let n=Un(t),e=Wc(t),{args:i,keys:r}=sl(t);if(i.length===0)return Ne([],n);let o=new te(QD(i,n,r?s=>al(r,s):Kt));return e?o.pipe(ol(e)):o}function QD(t,n,e=Kt){return i=>{Lv(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)Lv(n,()=>{let l=Ne(t[c],n),d=!1;l.subscribe(ne(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Lv(t,n,e){t?Gt(e,t,n):n()}function Vv(t,n,e,i,r,o,s,a){let c=[],l=0,d=0,f=!1,m=()=>{f&&!c.length&&!l&&n.complete()},g=x=>l<i?b(x):c.push(x),b=x=>{o&&n.next(x),l++;let R=!1;Ie(e(x,d++)).subscribe(ne(n,ee=>{r?.(ee),o?g(ee):n.next(ee)},()=>{R=!0},void 0,()=>{if(R)try{for(l--;c.length&&l<i;){let ee=c.shift();s?Gt(n,s,()=>b(ee)):b(ee)}m()}catch(ee){n.error(ee)}}))};return t.subscribe(ne(n,g,()=>{f=!0,m()})),()=>{a?.()}}function Lt(t,n,e=1/0){return K(n)?Lt((i,r)=>X((o,s)=>n(i,o,r,s))(Ie(t(i,r))),e):(typeof n=="number"&&(e=n),Q((i,r)=>Vv(i,r,t,e)))}function cl(t=1/0){return Lt(Kt,t)}function jv(){return cl(1)}function So(...t){return jv()(Ne(t,Un(t)))}function xr(t){return new te(n=>{Ie(t()).subscribe(n)})}function zs(...t){let n=Wc(t),{args:e,keys:i}=sl(t),r=new te(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;Ie(e[d]).subscribe(ne(o,m=>{f||(f=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?al(i,a):a),o.complete())}))}});return n?r.pipe(ol(n)):r}function Bv(t=0,n,e=wv){let i=-1;return n!=null&&(Gc(n)?e=n:i=n),new te(r=>{let o=Pv(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Hn(...t){let n=Un(t),e=Dv(t,1/0),i=t;return i.length?i.length===1?Ie(i[0]):cl(e)(Ne(i,n)):He}function Te(t,n){return Q((e,i)=>{let r=0;e.subscribe(ne(i,o=>t.call(n,o,r++)&&i.next(o)))})}function Uv(t){return Q((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(ne(e,l=>{i=!0,r=l,o||Ie(t(l)).subscribe(o=ne(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function ll(t,n=Bs){return Uv(()=>Bv(t,n))}function Ir(t){return Q((n,e)=>{let i=null,r=!1,o;i=n.subscribe(ne(e,void 0,void 0,s=>{o=Ie(t(s,Ir(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Mo(t,n){return K(n)?Lt(t,n,1):Lt(t,1)}function Mf(t,n=Bs){return Q((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}e.subscribe(ne(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Hv(t){return Q((n,e)=>{let i=!1;n.subscribe(ne(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Qt(t){return t<=0?()=>He:Q((n,e)=>{let i=0;n.subscribe(ne(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function dl(t,n=Kt){return t=t??XD,Q((e,i)=>{let r,o=!0;e.subscribe(ne(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function XD(t,n){return t===n}function zv(t=JD){return Q((n,e)=>{let i=!1;n.subscribe(ne(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function JD(){return new Dr}function Sr(t){return Q((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function di(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Te((r,o)=>t(r,o,i)):Kt,Qt(1),e?Hv(n):zv(()=>new Dr))}function ul(t){return t<=0?()=>He:Q((n,e)=>{let i=[];n.subscribe(ne(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function fl(){return Q((t,n)=>{let e,i=!1;t.subscribe(ne(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function $s(t={}){let{connector:n=()=>new S,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,d=!1,f=!1,m=()=>{a?.unsubscribe(),a=void 0},g=()=>{m(),s=c=void 0,d=f=!1},b=()=>{let x=s;g(),x?.unsubscribe()};return Q((x,R)=>{l++,!f&&!d&&m();let ee=c=c??n();R.add(()=>{l--,l===0&&!f&&!d&&(a=Tf(b,r))}),ee.subscribe(R),!s&&l>0&&(s=new li({next:it=>ee.next(it),error:it=>{f=!0,m(),a=Tf(g,e,it),ee.error(it)},complete:()=>{d=!0,m(),a=Tf(g,i),ee.complete()}}),Ie(x).subscribe(s))})(o)}}function Tf(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new li({next:()=>{i.unsubscribe(),t()}});return Ie(n(...e)).subscribe(i)}function hl(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,$s({connector:()=>new Uc(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Af(t){return Te((n,e)=>t<=e)}function Xt(...t){let n=Un(t);return Q((e,i)=>{(n?So(t,e,n):So(t,e)).subscribe(i)})}function ot(t,n){return Q((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(ne(i,c=>{r?.unsubscribe();let l=0,d=o++;Ie(t(c,d)).subscribe(r=ne(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ve(t){return Q((n,e)=>{Ie(t).subscribe(ne(e,()=>e.complete(),Ls)),!e.closed&&n.subscribe(e)})}function Rf(t,n=!1){return Q((e,i)=>{let r=0;e.subscribe(ne(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function ze(t,n,e){let i=K(t)||n||e?{next:t,error:n,complete:e}:t;return i?Q((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(ne(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Kt}var bl="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",E=class extends Error{code;constructor(n,e){super(hi(n,e)),this.code=n}};function ex(t){return`NG0${Math.abs(t)}`}function hi(t,n){return`${ex(t)}${n?": "+n:""}`}function _e(t){for(let n in t)if(t[n]===_e)return n;throw Error("")}function Yv(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Ks(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Ks).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Cl(t,n){return t?n?`${t} ${n}`:t:n||""}var tx=_e({__forward_ref__:_e});function ln(t){return t.__forward_ref__=ln,t}function gt(t){return $f(t)?t():t}function $f(t){return typeof t=="function"&&t.hasOwnProperty(tx)&&t.__forward_ref__===ln}function F(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function $(t){return{providers:t.providers||[],imports:t.imports||[]}}function Qs(t){return nx(t,El)}function Gf(t){return Qs(t)!==null}function nx(t,n){return t.hasOwnProperty(n)&&t[n]||null}function ix(t){let n=t?.[El]??null;return n||null}function kf(t){return t&&t.hasOwnProperty(ml)?t[ml]:null}var El=_e({\u0275prov:_e}),ml=_e({\u0275inj:_e}),_=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=F({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Wf(t){return t&&!!t.\u0275providers}var qf=_e({\u0275cmp:_e}),Yf=_e({\u0275dir:_e}),Zf=_e({\u0275pipe:_e}),Kf=_e({\u0275mod:_e}),Ws=_e({\u0275fac:_e}),kr=_e({__NG_ELEMENT_ID__:_e}),$v=_e({__NG_ENV_ID__:_e});function Zv(t){return wl(t,"@NgModule"),t[Kf]||null}function Vi(t){return wl(t,"@Component"),t[qf]||null}function Qf(t){return wl(t,"@Directive"),t[Yf]||null}function Kv(t){return wl(t,"@Pipe"),t[Zf]||null}function wl(t,n){if(t==null)throw new E(-919,!1)}function Dl(t){return typeof t=="string"?t:t==null?"":String(t)}var Qv=_e({ngErrorCode:_e}),rx=_e({ngErrorMessage:_e}),ox=_e({ngTokenPath:_e});function Xf(t,n){return Xv("",-200,n)}function xl(t,n){throw new E(-201,!1)}function Xv(t,n,e){let i=new E(n,t);return i[Qv]=n,i[rx]=t,e&&(i[ox]=e),i}function sx(t){return t[Qv]}var Of;function Jv(){return Of}function Jt(t){let n=Of;return Of=t,n}function Jf(t,n,e){let i=Qs(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;xl(t,"")}var ji=globalThis;var ax={},Mr=ax,cx="__NG_DI_FLAG__",Ff=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Tr(e)||0;try{return this.injector.get(n,i&8?null:Mr,i)}catch(r){if(Co(r))return r;throw r}}};function lx(t,n=0){let e=Fc();if(e===void 0)throw new E(-203,!1);if(e===null)return Jf(t,void 0,n);{let i=dx(n),r=e.retrieve(t,i);if(Co(r)){if(i.optional)return null;throw r}return r}}function M(t,n=0){return(Jv()||lx)(gt(t),n)}function u(t,n){return M(t,Tr(n))}function Tr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function dx(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Pf(t){let n=[];for(let e=0;e<t.length;e++){let i=gt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new E(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=ux(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(M(r,o))}else n.push(M(i))}return n}function ux(t){return t[cx]}function Ar(t,n){let e=t.hasOwnProperty(Ws);return e?t[Ws]:null}function ey(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function ty(t){return t.flat(Number.POSITIVE_INFINITY)}function Il(t,n){t.forEach(e=>Array.isArray(e)?Il(e,n):n(e))}function eh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Xs(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function ny(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function iy(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Sl(t,n,e){let i=Ro(t,n);return i>=0?t[i|1]=e:(i=~i,iy(t,i,n,e)),i}function Ml(t,n){let e=Ro(t,n);if(e>=0)return t[e|1]}function Ro(t,n){return fx(t,n,1)}function fx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Bi={},wt=[],Or=new _(""),Js=new _("",-1),th=new _(""),Ao=class{get(n,e=Mr){if(e===Mr){let r=Xv("",-201);throw r.name="\u0275NotFound",r}return e}};function pi(t){return{\u0275providers:t}}function ry(t){return pi([{provide:Or,multi:!0,useValue:t}])}function oy(...t){return{\u0275providers:nh(!0,t),\u0275fromNgModule:!0}}function nh(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Il(n,s=>{let a=s;gl(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&sy(r,o),e}function sy(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];ih(r,o=>{n(o,i)})}}function gl(t,n,e,i){if(t=gt(t),!t)return!1;let r=null,o=kf(t),s=!o&&Vi(t);if(!o&&!s){let c=t.ngModule;if(o=kf(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)gl(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Il(o.imports,d=>{gl(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&sy(l,n)}if(!a){let l=Ar(r)||(()=>new r);n({provide:r,useFactory:l,deps:wt},r),n({provide:th,useValue:r,multi:!0},r),n({provide:Or,useValue:()=>M(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;ih(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function ih(t,n){for(let e of t)Wf(e)&&(e=e.\u0275providers),Array.isArray(e)?ih(e,n):n(e)}var hx=_e({provide:String,useValue:_e});function ay(t){return t!==null&&typeof t=="object"&&hx in t}function px(t){return!!(t&&t.useExisting)}function mx(t){return!!(t&&t.useFactory)}function Rr(t){return typeof t=="function"}function cy(t){return!!t.useClass}var ea=new _(""),pl={},Gv={},Nf;function No(){return Nf===void 0&&(Nf=new Ao),Nf}var Ee=class{},Nr=class extends Ee{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Vf(n,s=>this.processProvider(s)),this.records.set(Js,To(void 0,this)),r.has("environment")&&this.records.set(Ee,To(void 0,this));let o=this.records.get(ea);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(th,wt,{self:!0}))}retrieve(n,e){let i=Tr(e)||0;try{return this.get(n,Mr,i)}catch(r){if(Co(r))return r;throw r}}destroy(){Gs(this),this._destroyed=!0;let n=U(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),U(n)}}onDestroy(n){return Gs(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Gs(this);let e=Bn(this),i=Jt(void 0),r;try{return n()}finally{Bn(e),Jt(i)}}get(n,e=Mr,i){if(Gs(this),n.hasOwnProperty($v))return n[$v](this);let r=Tr(i),o,s=Bn(this),a=Jt(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=bx(n)&&Qs(n);d&&this.injectableDefInScope(d)?l=To(Lf(n),pl):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?No():this.parent;return e=r&8&&e===Mr?null:e,c.get(n,e)}catch(c){let l=sx(c);throw l===-200||l===-201?new E(l,null):c}finally{Jt(a),Bn(s)}}resolveInjectorInitializers(){let n=U(null),e=Bn(this),i=Jt(void 0),r;try{let o=this.get(Or,wt,{self:!0});for(let s of o)s()}finally{Bn(e),Jt(i),U(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=gt(n);let e=Rr(n)?n:gt(n&&n.provide),i=vx(n);if(!Rr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=To(void 0,pl,!0),r.factory=()=>Pf(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=U(null);try{if(e.value===Gv)throw Xf("");return e.value===pl&&(e.value=Gv,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&_x(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{U(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=gt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Lf(t){let n=Qs(t),e=n!==null?n.factory:Ar(t);if(e!==null)return e;if(t instanceof _)throw new E(-204,!1);if(t instanceof Function)return gx(t);throw new E(-204,!1)}function gx(t){if(t.length>0)throw new E(-204,!1);let e=ix(t);return e!==null?()=>e.factory(t):()=>new t}function vx(t){if(ay(t))return To(void 0,t.useValue);{let n=rh(t);return To(n,pl)}}function rh(t,n,e){let i;if(Rr(t)){let r=gt(t);return Ar(r)||Lf(r)}else if(ay(t))i=()=>gt(t.useValue);else if(mx(t))i=()=>t.useFactory(...Pf(t.deps||[]));else if(px(t))i=(r,o)=>M(gt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=gt(t&&(t.useClass||t.provide));if(yx(t))i=()=>new r(...Pf(t.deps));else return Ar(r)||Lf(r)}return i}function Gs(t){if(t.destroyed)throw new E(-205,!1)}function To(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function yx(t){return!!t.deps}function _x(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function bx(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Vf(t,n){for(let e of t)Array.isArray(e)?Vf(e,n):e&&Wf(e)?Vf(e.\u0275providers,n):n(e)}function ut(t,n){let e;t instanceof Nr?(Gs(t),e=t):e=new Ff(t);let i,r=Bn(e),o=Jt(void 0);try{return n()}finally{Bn(r),Jt(o)}}function ly(){return Jv()!==void 0||Fc()!=null}var An=0,k=1,H=2,dt=3,dn=4,Dt=5,ko=6,Oo=7,vt=8,mi=9,Rn=10,Se=11,Fo=12,oh=13,Ui=14,Wt=15,Hi=16,Fr=17,$n=18,gi=19,sh=20,ui=21,Tl=22,Li=23,en=24,Pr=25,zi=26,$e=27,dy=1;var $i=7,ta=8,Lr=9,yt=10;function vi(t){return Array.isArray(t)&&typeof t[dy]=="object"}function un(t){return Array.isArray(t)&&t[dy]===!0}function ah(t){return(t.flags&4)!==0}function yi(t){return t.componentOffset>-1}function Po(t){return(t.flags&1)===1}function Gn(t){return!!t.template}function Lo(t){return(t[H]&512)!==0}function Vr(t){return(t[H]&256)===256}var ch="svg",uy="math";function fn(t){for(;Array.isArray(t);)t=t[An];return t}function lh(t,n){return fn(n[t])}function hn(t,n){return fn(n[t.index])}function Al(t,n){return t.data[n]}function fy(t,n){return t[n]}function pn(t,n){let e=n[t];return vi(e)?e:e[An]}function hy(t){return(t[H]&4)===4}function Rl(t){return(t[H]&128)===128}function py(t){return un(t[dt])}function Wn(t,n){return n==null?null:t[n]}function dh(t){t[Fr]=0}function uh(t){t[H]&1024||(t[H]|=1024,Rl(t)&&jr(t))}function my(t,n){for(;t>0;)n=n[Ui],t--;return n}function na(t){return!!(t[H]&9216||t[en]?.dirty)}function Nl(t){t[Rn].changeDetectionScheduler?.notify(8),t[H]&64&&(t[H]|=1024),na(t)&&jr(t)}function jr(t){t[Rn].changeDetectionScheduler?.notify(0);let n=fi(t);for(;n!==null&&!(n[H]&8192||(n[H]|=8192,!Rl(n)));)n=fi(n)}function kl(t,n){if(Vr(t))throw new E(911,!1);t[ui]===null&&(t[ui]=[]),t[ui].push(n)}function gy(t,n){if(t[ui]===null)return;let e=t[ui].indexOf(n);e!==-1&&t[ui].splice(e,1)}function fi(t){let n=t[dt];return un(n)?n[dt]:n}function fh(t){return t[Oo]??=[]}function hh(t){return t.cleanup??=[]}function vy(t,n,e,i){let r=fh(n);r.push(e),t.firstCreatePass&&hh(t).push(i,r.length-1)}var ie={lFrame:Ty(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var jf=!1;function yy(){return ie.lFrame.elementDepthCount}function _y(){ie.lFrame.elementDepthCount++}function ph(){ie.lFrame.elementDepthCount--}function Ol(){return ie.bindingsEnabled}function mh(){return ie.skipHydrationRootTNode!==null}function gh(t){return ie.skipHydrationRootTNode===t}function vh(){ie.skipHydrationRootTNode=null}function Y(){return ie.lFrame.lView}function ke(){return ie.lFrame.tView}function ge(t){return ie.lFrame.contextLView=t,t[vt]}function ve(t){return ie.lFrame.contextLView=null,t}function Ye(){let t=yh();for(;t!==null&&t.type===64;)t=t.parent;return t}function yh(){return ie.lFrame.currentTNode}function by(){let t=ie.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Vo(t,n){let e=ie.lFrame;e.currentTNode=t,e.isParent=n}function _h(){return ie.lFrame.isParent}function bh(){ie.lFrame.isParent=!1}function Cy(){return ie.lFrame.contextLView}function Ch(){return jf}function qs(t){let n=jf;return jf=t,n}function Ey(){let t=ie.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function wy(t){return ie.lFrame.bindingIndex=t}function Br(){return ie.lFrame.bindingIndex++}function Eh(t){let n=ie.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Dy(){return ie.lFrame.inI18n}function xy(t,n){let e=ie.lFrame;e.bindingIndex=e.bindingRootIndex=t,Fl(n)}function Iy(){return ie.lFrame.currentDirectiveIndex}function Fl(t){ie.lFrame.currentDirectiveIndex=t}function Sy(t){let n=ie.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Pl(){return ie.lFrame.currentQueryIndex}function ia(t){ie.lFrame.currentQueryIndex=t}function Cx(t){let n=t[k];return n.type===2?n.declTNode:n.type===1?t[Dt]:null}function wh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Cx(o),r===null||(o=o[Ui],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ie.lFrame=My();return i.currentTNode=n,i.lView=t,!0}function Ll(t){let n=My(),e=t[k];ie.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function My(){let t=ie.lFrame,n=t===null?null:t.child;return n===null?Ty(t):n}function Ty(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Ay(){let t=ie.lFrame;return ie.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Dh=Ay;function Vl(){let t=Ay();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Ry(t){return(ie.lFrame.contextLView=my(t,ie.lFrame.contextLView))[vt]}function Gi(){return ie.lFrame.selectedIndex}function Wi(t){ie.lFrame.selectedIndex=t}function jo(){let t=ie.lFrame;return Al(t.tView,t.selectedIndex)}function qi(){ie.lFrame.currentNamespace=ch}function jl(){Ex()}function Ex(){ie.lFrame.currentNamespace=null}function xh(){return ie.lFrame.currentNamespace}var Ny=!0;function Bl(){return Ny}function ra(t){Ny=t}function Bf(t,n=null,e=null,i){let r=Ih(t,n,e,i);return r.resolveInjectorInitializers(),r}function Ih(t,n=null,e=null,i,r=new Set){let o=[e||wt,oy(t)],s;return new Nr(o,n||No(),s||null,r)}var ce=class t{static THROW_IF_NOT_FOUND=Mr;static NULL=new Ao;static create(n,e){if(Array.isArray(n))return Bf({name:""},e,n,"");{let i=n.name??"";return Bf({name:i},n.parent,n.providers,i)}}static \u0275prov=F({token:t,providedIn:"any",factory:()=>M(Js)});static __NG_ELEMENT_ID__=-1},V=new _(""),_t=(()=>{class t{static __NG_ELEMENT_ID__=wx;static __NG_ENV_ID__=e=>e}return t})(),vl=class extends _t{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Vr(this._lView)}onDestroy(n){let e=this._lView;return kl(e,n),()=>gy(e,n)}};function wx(){return new vl(Y())}var ky=!1,Oy=new _(""),_i=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new rt(!1);debugTaskTracker=u(Oy,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new te(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),Uf=class extends S{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,ly()&&(this.destroyRef=u(_t,{optional:!0})??void 0,this.pendingTasks=u(_i,{optional:!0})??void 0)}emit(n){let e=U(null);try{super.next(n)}finally{U(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ue&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},J=Uf;function yl(...t){}function Sh(t){let n,e;function i(){t=yl;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch(r){}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Fy(t){return queueMicrotask(()=>t()),()=>{t=yl}}var Mh="isAngularZone",Ys=Mh+"_ID",Dx=0,O=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new J(!1);onMicrotaskEmpty=new J(!1);onStable=new J(!1);onError=new J(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=ky}=n;if(typeof Zone>"u")throw new E(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,Sx(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Mh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new E(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new E(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,xx,yl,yl);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},xx={};function Th(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Ix(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Sh(()=>{t.callbackScheduled=!1,Hf(t),t.isCheckStableRunning=!0,Th(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Hf(t)}function Sx(t){let n=()=>{Ix(t)},e=Dx++;t._inner=t._inner.fork({name:"angular",properties:{[Mh]:!0,[Ys]:e,[Ys+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(Mx(c))return i.invokeTask(o,s,a,c);try{return Wv(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),qv(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return Wv(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!Tx(c)&&n(),qv(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Hf(t),Th(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Hf(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Wv(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function qv(t){t._nesting--,Th(t)}var Zs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new J;onMicrotaskEmpty=new J;onStable=new J;onError=new J;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function Mx(t){return Py(t,"__ignore_ng_zone__")}function Tx(t){return Py(t,"__scheduler_tick__")}function Py(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Vt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},tn=new _("",{factory:()=>{let t=u(O),n=u(Ee),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Vt),e.handleError(i))})}}}),Ly={provide:Or,useValue:()=>{let t=u(Vt,{optional:!0})},multi:!0},Ax=new _("",{factory:()=>{let t=u(V).defaultView;if(!t)return;let n=u(tn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(_t).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Ah(){return pi([ry(()=>{u(Ax)})])}function re(t,n){let[e,i,r]=hf(t,n?.equal),o=e,s=o[Ue];return o.set=i,o.update=r,o.asReadonly=Ul.bind(o),o}function Ul(){let t=this[Ue];if(t.readonlyFn===void 0){let n=()=>this();n[Ue]=t,t.readonlyFn=n}return t.readonlyFn}var Ur=new _("",{factory:()=>Rx}),Rx="ng";var Hl=new _(""),Hr=new _("",{providedIn:"platform",factory:()=>"unknown"}),oa=new _(""),zr=new _("",{factory:()=>u(V).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Bo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Nx}return t})();function Nx(){return new Bo(Y(),Ye())}var zn=class{},sa=new _("",{factory:()=>!0});var Rh=new _(""),zl=(()=>{class t{static \u0275prov=F({token:t,providedIn:"root",factory:()=>new zf})}return t})(),zf=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},_l=class{[Ue];constructor(n){this[Ue]=n}destroy(){this[Ue].destroy()}};function Nn(t,n){let e=n?.injector??u(ce),i=n?.manualCleanup!==!0?e.get(_t):null,r,o=e.get(Bo,null,{optional:!0}),s=e.get(zn);return o!==null?(r=Fx(o.view,s,t),i instanceof vl&&i._lView===o.view&&(i=null)):r=Px(t,e.get(zl),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new _l(r)}var Vy=B(C({},pf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=qs(!1);try{mf(this)}finally{qs(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=U(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],U(t)}}}),kx=B(C({},Vy),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Pi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Ox=B(C({},Vy),{consumerMarkedDirty(){this.view[H]|=8192,jr(this.view),this.notifier.notify(13)},destroy(){if(Pi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Li]?.delete(this)}});function Fx(t,n,e){let i=Object.create(Ox);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=jy(i,e),t[Li]??=new Set,t[Li].add(i),i.consumerMarkedDirty(i),i}function Px(t,n,e){let i=Object.create(kx);return i.fn=jy(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function jy(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function mn(t){return typeof t=="function"&&t[Ue]!==void 0}function $l(t){return mn(t)&&typeof t.set=="function"}var aa=(()=>{class t{internalPendingTasks=u(_i);scheduler=u(zn);errorHandler=u(tn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})();function ga(t){return{toString:t}.toString()}var he=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(he||{}),Jl=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function D_(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var x_=null,Ke=(()=>{x_=By;let t=()=>By;return t.ngInherit=!0,t})();function Wx(){return x_}function By(t){return t.type.prototype.ngOnChanges&&(t.setInput=Yx),qx}function qx(){let t=I_(this),n=t?.current;if(n){let e=t.previous;if(e===Bi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function Yx(t,n,e,i,r){let o=this.declaredInputs[i],s=I_(t)||Zx(t,{previous:Bi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Jl(l&&l.currentValue,e,c===Bi),D_(t,n,r,e)}var Uh="__ngSimpleChanges__";function I_(t){return Object.hasOwn(t,Uh)&&t[Uh]||null}function Zx(t,n){return t[Uh]=n}var Uy=[];var be=function(t,n=null,e){for(let i=0;i<Uy.length;i++){let r=Uy[i];r(t,n,e)}};function Kx(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=Wx()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function S_(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function Zl(t,n,e){M_(t,n,3,e)}function Kl(t,n,e,i){(t[H]&3)===e&&M_(t,n,e,i)}function Nh(t,n){let e=t[H];(e&3)===n&&(e&=16383,e+=1,t[H]=e)}function M_(t,n,e,i){let r=i!==void 0?t[Fr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[Fr]+=65536),(a<o||o==-1)&&(Qx(t,e,n,c),t[Fr]=(t[Fr]&4294901760)+c+2),c++}function Hy(t,n){be(he.LifecycleHookStart,t,n);let e=U(null);try{n.call(t)}finally{U(e),be(he.LifecycleHookEnd,t,n)}}function Qx(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[H]>>14<t[Fr]>>16&&(t[H]&3)===n&&(t[H]+=16384,Hy(a,o)):Hy(a,o)}var Ho=-1,$r=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function Xx(t){return(t.flags&8)!==0}function Jx(t){return(t.flags&16)!==0}function eI(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];tI(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function T_(t){return t===3||t===4||t===6}function tI(t){return t.charCodeAt(0)===64}function $o(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?zy(t,e,r,null,n[++i]):zy(t,e,r,null,null))}}return t}function zy(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function A_(t){return t!==Ho}function ed(t){return t&32767}function nI(t){return t>>16}function td(t,n){let e=nI(t),i=n;for(;e>0;)i=i[Ui],e--;return i}var Hh=!0;function $y(t){let n=Hh;return Hh=t,n}var iI=256,R_=iI-1,N_=5,rI=0,qn={};function oI(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(kr)&&(i=e[kr]),i==null&&(i=e[kr]=rI++);let r=i&R_,o=1<<r;n.data[t+(r>>N_)]|=o}function nd(t,n){let e=k_(t,n);if(e!==-1)return e;let i=n[k];i.firstCreatePass&&(t.injectorIndex=n.length,kh(i.data,t),kh(n,null),kh(i.blueprint,null));let r=vp(t,n),o=t.injectorIndex;if(A_(r)){let s=ed(r),a=td(r,n),c=a[k].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function kh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function k_(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function vp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=V_(r),i===null)return Ho;if(e++,r=r[Ui],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ho}function zh(t,n,e){oI(t,n,e)}function sI(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(T_(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function O_(t,n,e){if(e&8||t!==void 0)return t;xl(n,"NodeInjector")}function F_(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[mi],o=Jt(void 0);try{return r?r.get(n,i,e&8):Jf(n,i,e&8)}finally{Jt(o)}}return O_(i,n,e)}function P_(t,n,e,i=0,r){if(t!==null){if(n[H]&2048&&!(i&2)){let s=dI(t,n,e,i,qn);if(s!==qn)return s}let o=L_(t,n,e,i,qn);if(o!==qn)return o}return F_(n,e,i,r)}function L_(t,n,e,i,r){let o=cI(e);if(typeof o=="function"){if(!wh(n,t,i))return i&1?O_(r,e,i):F_(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))xl(e);else return s}finally{Dh()}}else if(typeof o=="number"){let s=null,a=k_(t,n),c=Ho,l=i&1?n[Wt][Dt]:null;for((a===-1||i&4)&&(c=a===-1?vp(t,n):n[a+8],c===Ho||!Wy(i,!1)?a=-1:(s=n[k],a=ed(c),n=td(c,n)));a!==-1;){let d=n[k];if(Gy(o,a,d.data)){let f=aI(a,n,e,s,i,l);if(f!==qn)return f}c=n[a+8],c!==Ho&&Wy(i,n[k].data[a+8]===l)&&Gy(o,a,n)?(s=d,a=ed(c),n=td(c,n)):a=-1}}return r}function aI(t,n,e,i,r,o){let s=n[k],a=s.data[t+8],c=i==null?yi(a)&&Hh:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=Ql(a,s,e,c,l);return d!==null?ua(n,s,d,a,r):qn}function Ql(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,f=i?a:a+d,m=r?a+d:l;for(let g=f;g<m;g++){let b=s[g];if(g<c&&e===b||g>=c&&b.type===e)return g}if(r){let g=s[c];if(g&&Gn(g)&&g.type===e)return c}return null}function ua(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof $r){let a=o;if(a.resolving)throw Xf("");let c=$y(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,f=a.injectImpl?Jt(a.injectImpl):null,m=wh(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&Kx(e,s[e],n)}finally{f!==null&&Jt(f),$y(c),a.resolving=!1,Dh()}}return o}function cI(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(kr)?t[kr]:void 0;return typeof n=="number"?n>=0?n&R_:lI:n}function Gy(t,n,e){let i=1<<t;return!!(e[n+(t>>N_)]&i)}function Wy(t,n){return!(t&2)&&!(t&1&&n)}var Yi=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return P_(this._tNode,this._lView,n,Tr(i),e)}};function lI(){return new Yi(Ye(),Y())}function Zn(t){return ga(()=>{let n=t.prototype.constructor,e=n[Ws]||$h(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Ws]||$h(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function $h(t){return $f(t)?()=>{let n=$h(gt(t));return n&&n()}:Ar(t)}function dI(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[H]&2048&&!Lo(s);){let a=L_(o,s,e,i|2,qn);if(a!==qn)return a;let c=o.parent;if(!c){let l=s[sh];if(l){let d=l.get(e,qn,i&-5);if(d!==qn)return d}c=V_(s),s=s[Ui]}o=c}return r}function V_(t){let n=t[k],e=n.type;return e===2?n.declTNode:e===1?t[Dt]:null}function va(t){return sI(Ye(),t)}function T(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function uI(){return Yo(Ye(),Y())}function Yo(t,n){return new z(hn(t,n))}var z=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=uI}return t})();function j_(t){return t instanceof z?t.nativeElement:t}function fI(){return this._results[Symbol.iterator]()}var bi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new S}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=ty(n);(this._changesDetected=!ey(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=fI};function B_(t){return(t.flags&128)===128}var yp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(yp||{}),U_=new Map,hI=0;function pI(){return hI++}function mI(t){U_.set(t[gi],t)}function Gh(t){U_.delete(t[gi])}var qy="__ngContext__";function Go(t,n){vi(n)?(t[qy]=n[gi],mI(n)):t[qy]=n}function H_(t){return $_(t[Fo])}function z_(t){return $_(t[dn])}function $_(t){for(;t!==null&&!un(t);)t=t[dn];return t}var Wh;function _p(t){Wh=t}function G_(){if(Wh!==void 0)return Wh;if(typeof document<"u")return document;throw new E(210,!1)}var W_=!1,q_=new _("",{factory:()=>W_});var Yy=new WeakMap;function gI(t,n){if(t==null||typeof t!="object")return;let e=Yy.get(t);e||(e=new WeakSet,Yy.set(t,e)),e.add(n)}var vI=(t,n,e,i)=>{};function yI(t,n,e,i){vI(t,n,e,i)}function md(t){return(t.flags&32)===32}var _I=()=>null;function Y_(t,n,e=!1){return _I(t,n,e)}function Z_(t,n){let e=t.contentQueries;if(e!==null){let i=U(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ia(o),a.contentQueries(2,n[s],s)}}}finally{U(i)}}}function qh(t,n,e){ia(0);let i=U(null);try{n(t,e)}finally{U(i)}}function bp(t,n,e){if(ah(n)){let i=U(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{U(i)}}}var Fn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Fn||{});var Gl;function bI(){if(Gl===void 0&&(Gl=null,ji.trustedTypes))try{Gl=ji.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return Gl}function gd(t){return bI()?.createHTML(t)||t}var Wl;function CI(){if(Wl===void 0&&(Wl=null,ji.trustedTypes))try{Wl=ji.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return Wl}function Zy(t){return CI()?.createScriptURL(t)||t}var Ci=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${bl})`}},Yh=class extends Ci{getTypeName(){return"HTML"}},Zh=class extends Ci{getTypeName(){return"Style"}},Kh=class extends Ci{getTypeName(){return"Script"}},Qh=class extends Ci{getTypeName(){return"URL"}},Xh=class extends Ci{getTypeName(){return"ResourceURL"}};function Pn(t){return t instanceof Ci?t.changingThisBreaksApplicationSecurity:t}function wi(t,n){let e=K_(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${bl})`)}return e===n}function K_(t){return t instanceof Ci&&t.getTypeName()||null}function Cp(t){return new Yh(t)}function Ep(t){return new Zh(t)}function wp(t){return new Kh(t)}function Dp(t){return new Qh(t)}function xp(t){return new Xh(t)}function EI(t){let n=new ep(t);return wI()?new Jh(n):n}var Jh=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(gd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch(e){return null}}},ep=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=gd(n),e}};function wI(){try{return!!new window.DOMParser().parseFromString(gd(""),"text/html")}catch(t){return!1}}var DI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function ya(t){return t=String(t),t.match(DI)?t:"unsafe:"+t}function Di(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function _a(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var Q_=Di("area,br,col,hr,img,wbr"),X_=Di("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),J_=Di("rp,rt"),xI=_a(J_,X_),II=_a(X_,Di("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),SI=_a(J_,Di("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Ky=_a(Q_,II,SI,xI),eb=Di("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),MI=Di("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),TI=Di("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),AI=_a(eb,MI,TI),RI=Di("script,style,template"),tp=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=OI(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=kI(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=Qy(n).toLowerCase();if(!Ky.hasOwnProperty(e))return this.sanitizedSomething=!0,!RI.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!AI.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;eb[a]&&(c=ya(c)),this.buf.push(" ",s,'="',Xy(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=Qy(n).toLowerCase();Ky.hasOwnProperty(e)&&!Q_.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Xy(n))}};function NI(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function kI(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw tb(n);return n}function OI(t){let n=t.firstChild;if(n&&NI(t,n))throw tb(n);return n}function Qy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function tb(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var FI=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,PI=/([^\#-~ |!])/g;function Xy(t){return t.replace(/&/g,"&amp;").replace(FI,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(PI,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var ql;function Ip(t,n){let e=null;try{ql=ql||EI(t);let i=n?String(n):"";e=ql.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=ql.getInertBodyElement(i)}while(i!==o);let a=new tp().sanitizeChildren(Jy(e)||e);return gd(a)}finally{if(e){let i=Jy(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Jy(t){return"content"in t&&LI(t)?t.content:null}function LI(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var VI=/^>|^->|<!--|-->|--!>|<!-$/g,jI=/(<|>)/g,BI="\u200B$1\u200B";function UI(t){return t.replace(VI,n=>n.replace(jI,BI))}function HI(t,n){return t.createText(n)}function zI(t,n,e){t.setValue(n,e)}function $I(t,n){return t.createComment(UI(n))}function nb(t,n,e){return t.createElement(n,e)}function id(t,n,e,i,r){t.insertBefore(n,e,i,r)}function ib(t,n,e){t.appendChild(n,e)}function e_(t,n,e,i,r){i!==null?id(t,n,e,i,r):ib(t,n,e)}function GI(t,n,e,i){t.removeChild(null,n,e,i)}function WI(t,n,e){t.setAttribute(n,"style",e)}function qI(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function rb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&eI(t,n,i),r!==null&&qI(t,n,r),o!==null&&WI(t,n,o)}var st=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(st||{});function ob(t){let n=ab();return n?n.sanitize(st.URL,t)||"":wi(t,"URL")?Pn(t):ya(Dl(t))}function sb(t){let n=ab();if(n)return Zy(n.sanitize(st.RESOURCE_URL,t)||"");if(wi(t,"ResourceURL"))return Zy(Pn(t));throw new E(904,!1)}var YI={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function ZI(t,n){return YI[t.toLowerCase()]?.[n.toLowerCase()]===!0?sb:ob}function Sp(t,n,e){return ZI(n,e)(t)}function ab(){let t=Y();return t&&t[Rn].sanitizer}function KI(t){return t instanceof Function?t():t}function QI(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var cb="ng-template";function XI(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&QI(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Mp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Mp(t){return t.type===4&&t.value!==cb}function JI(t,n,e){let i=t.type===4&&!e?cb:t.value;return n===i}function eS(t,n,e){let i=4,r=t.attrs,o=r!==null?iS(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!kn(i)&&!kn(c))return!1;if(s&&kn(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!JI(t,c,e)||c===""&&n.length===1){if(kn(i))return!1;s=!0}}else if(i&8){if(r===null||!XI(t,r,c,e)){if(kn(i))return!1;s=!0}}else{let l=n[++a],d=tS(c,r,Mp(t),e);if(d===-1){if(kn(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(kn(i))return!1;s=!0}}}}return kn(i)||s}function kn(t){return(t&1)===0}function tS(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return rS(n,t)}function lb(t,n,e=!1){for(let i=0;i<n.length;i++)if(eS(t,n[i],e))return!0;return!1}function nS(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function iS(t){for(let n=0;n<t.length;n++){let e=t[n];if(T_(e))return n}return t.length}function rS(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function oS(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function t_(t,n){return t?":not("+n.trim()+")":n}function sS(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!kn(s)&&(n+=t_(o,r),r=""),i=s,o=o||!kn(i);e++}return r!==""&&(n+=t_(o,r)),n}function aS(t){return t.map(sS).join(",")}function cS(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!kn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var vn={},Yn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Yn||{}),lS;function Tp(t,n){return lS(t,n)}var zH=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var np=new WeakMap;function db(t){return t?t[Ui]??t:null}var ca=new WeakSet;function dS(t,n,e){let i=np.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=db(e);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),ca.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function uS(t,n,e){let i=db(e),r=np.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):np.set(t,[{el:n,declarationView:i}])}var Gr=new Set,vd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(vd||{}),Kn=new _(""),n_=new Set;function Ki(t){n_.has(t)||(n_.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var yd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ap=[0,1,2,3],Rp=(()=>{class t{ngZone=u(O);scheduler=u(zn);errorHandler=u(Vt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(Kn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&be(he.AfterRenderHooksStart),this.executing=!0;for(let i of Ap)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&be(he.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Pr]??=[]).push(e),jr(i),i[H]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(vd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=F({token:t,providedIn:"root",factory:()=>new t})}return t})(),fa=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Pr];n&&(this.view[Pr]=n.filter(e=>e!==this))}};function Qn(t,n){let e=n?.injector??u(ce);return Ki("NgAfterNextRender"),hS(t,e,n,!0)}function fS(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function hS(t,n,e,i){let r=n.get(yd);r.impl??=n.get(Rp);let o=n.get(Kn,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(_t):null,a=n.get(Bo,null,{optional:!0}),c=new fa(r.impl,fS(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var ub=new _("",{factory:()=>{let t=u(Ee),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function fb(t,n,e){let i=t.get(ub);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function pS(t,n){let e=t.get(ub);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function mS(t,n){for(let[e,i]of n)fb(t,i.animateFns)}function i_(t,n,e,i){let r=t?.[zi]?.enter;n!==null&&r&&r.has(e.index)&&mS(i,r)}function r_(t,n,e,i){try{e.get(Js)}catch(s){return i(!1)}let r=t?.[zi];r?.enter?.has(n.index)&&pS(e,r.enter.get(n.index).animateFns);let o=gS(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];_d(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Gr.add(t[gi]),fb(e,()=>vS(t,n,r||void 0,o,i),r||void 0)}function gS(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let c=t[k].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function vS(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&_d(t,n,o),o.length>0){let s=e||t?.[zi];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),_S(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Gr.delete(t[gi]),r(!0)})}else t&&Gr.delete(t[gi]),r(!1)}function _d(t,n,e){if(n.type&12){let r=t[n.index];if(un(r))for(let o=yt;o<r.length;o++){let s=r[o];s[k].type===2&&yS(s,e)}}let i=n.child;for(;i;)_d(t,i,e),i=i.next}function yS(t,n){let e=t[zi];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[k].firstChild;for(;i;)_d(t,i,n),i=i.next}function _S(t,n,e){n.then(()=>{t[zi]?.running===n&&(t[zi].running=void 0,Gr.delete(t[gi])),e(!0)})}function Uo(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;un(r)?c=r:vi(r)&&(l=!0,r=r[An]);let d=fn(r);t===0&&i!==null?(i_(a,i,o,e),s==null?ib(n,i,d):id(n,i,d,s||null,!0)):t===1&&i!==null?(i_(a,i,o,e),id(n,i,d,s||null,!0),dS(o,d,a)):t===2?(a?.[zi]?.leave?.has(o.index)&&uS(o,d,a),ca.delete(d),r_(a,o,e,f=>{if(ca.has(d)){ca.delete(d);return}GI(n,d,l,f)})):t===3&&(ca.delete(d),r_(a,o,e,()=>{n.destroyNode(d)})),c!=null&&TS(n,t,e,c,o,i,s)}}function bS(t,n){hb(t,n),n[An]=null,n[Dt]=null}function CS(t,n,e,i,r,o){i[An]=r,i[Dt]=n,bd(t,i,e,1,r,o)}function hb(t,n){n[Rn].changeDetectionScheduler?.notify(9),bd(t,n,n[Se],2,null,null)}function ES(t){let n=t[Fo];if(!n)return Oh(t[k],t);for(;n;){let e=null;if(vi(n))e=n[Fo];else{let i=n[yt];i&&(e=i)}if(!e){for(;n&&!n[dn]&&n!==t;)vi(n)&&Oh(n[k],n),n=n[dt];n===null&&(n=t),vi(n)&&Oh(n[k],n),e=n&&n[dn]}n=e}}function Np(t,n){let e=t[Lr],i=e.indexOf(n);e.splice(i,1)}function kp(t,n){if(Vr(n))return;let e=n[Se];e.destroyNode&&bd(t,n,e,3,null,null),ES(n)}function Oh(t,n){if(Vr(n))return;let e=U(null);try{n[H]&=-129,n[H]|=256,n[en]&&Pi(n[en]),DS(t,n),wS(t,n),n[k].type===1&&n[Se].destroy();let i=n[Hi];if(i!==null&&un(n[dt])){i!==n[dt]&&Np(i,n);let r=n[$n];r!==null&&r.detachView(t)}Gh(n)}finally{U(e)}}function wS(t,n){let e=t.cleanup,i=n[Oo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[Oo]=null);let r=n[ui];if(r!==null){n[ui]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Li];if(o!==null){n[Li]=null;for(let s of o)s.destroy()}}function DS(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof $r)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];be(he.LifecycleHookStart,a,c);try{c.call(a)}finally{be(he.LifecycleHookEnd,a,c)}}else{be(he.LifecycleHookStart,r,o);try{o.call(r)}finally{be(he.LifecycleHookEnd,r,o)}}}}}function pb(t,n,e){return xS(t,n.parent,e)}function xS(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[An];if(yi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Fn.None||r===Fn.Emulated)return null}return hn(i,e)}function mb(t,n,e){return SS(t,n,e)}function IS(t,n,e){return t.type&40?hn(t,e):null}var SS=IS,o_;function Op(t,n,e,i){let r=pb(t,i,n),o=n[Se],s=i.parent||n[Dt],a=mb(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)e_(o,r,e[c],a,!1);else e_(o,r,e,a,!1);o_!==void 0&&o_(o,i,n,e,r)}function la(t,n){if(n!==null){let e=n.type;if(e&3)return hn(n,t);if(e&4)return ip(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return la(t,i);{let r=t[n.index];return un(r)?ip(-1,r):fn(r)}}else{if(e&128)return la(t,n.next);if(e&32)return Tp(n,t)()||fn(t[n.index]);{let i=gb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=fi(t[Wt]);return la(r,i)}else return la(t,n.next)}}}return null}function gb(t,n){if(n!==null){let i=t[Wt][Dt],r=n.projection;return i.projection[r]}return null}function ip(t,n){let e=yt+t+1;if(e<n.length){let i=n[e],r=i[k].firstChild;if(r!==null)return la(i,r)}return n[$i]}function Fp(t,n,e,i,r,o,s){for(;e!=null;){let a=i[mi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&Go(fn(c),i),e.flags|=2),!md(e))if(l&8)Fp(t,n,e.child,i,r,o,!1),Uo(n,t,a,r,c,e,o,i);else if(l&32){let d=Tp(e,i),f;for(;f=d();)Uo(n,t,a,r,f,e,o,i);Uo(n,t,a,r,c,e,o,i)}else l&16?vb(t,n,i,e,r,o):Uo(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function bd(t,n,e,i,r,o){Fp(e,i,t.firstChild,n,r,o,!1)}function MS(t,n,e){let i=n[Se],r=pb(t,e,n),o=e.parent||n[Dt],s=mb(o,e,n);vb(i,0,n,e,r,s)}function vb(t,n,e,i,r,o){let s=e[Wt],c=s[Dt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Uo(n,t,e[mi],r,d,i,o,e)}else{let l=c,d=s[dt];B_(i)&&(l.flags|=128),Fp(t,n,l,d,r,o,!0)}}function TS(t,n,e,i,r,o,s){let a=i[$i],c=fn(i);a!==c&&Uo(n,t,e,o,a,r,s);for(let l=yt;l<i.length;l++){let d=i[l];bd(d[k],d,t,n,o,a)}}function AS(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Yn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Yn.Important),t.setStyle(e,i,r,o))}}function Pp(t,n,e,i,r,o,s,a,c,l,d){let f=$e+i,m=f+r,g=RS(f,m),b=typeof l=="function"?l():l;return g[k]={type:t,blueprint:g,template:e,queries:null,viewQuery:a,declTNode:n,data:g.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:b,incompleteFirstPass:!1,ssrId:d}}function RS(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:vn);return e}function NS(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Pp(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Lp(t,n,e,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[An]=r,f[H]=i|4|128|8|64|1024,(l!==null||t&&t[H]&2048)&&(f[H]|=2048),dh(f),f[dt]=f[Ui]=t,f[vt]=e,f[Rn]=s||t&&t[Rn],f[Se]=a||t&&t[Se],f[mi]=c||t&&t[mi]||null,f[Dt]=o,f[gi]=pI(),f[ko]=d,f[sh]=l,f[Wt]=n.type==2?t[Wt]:f,f}function kS(t,n,e){let i=hn(n,t),r=NS(e),o=t[Rn].rendererFactory,s=Vp(t,Lp(t,r,null,yb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function yb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function _b(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Vp(t,n){return t[Fo]?t[oh][dn]=n:t[Fo]=n,t[oh]=n,n}function v(t=1){bb(ke(),Y(),Gi()+t,!1)}function bb(t,n,e,i){if(!i)if((n[H]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Zl(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Kl(n,o,0,e)}Wi(e)}var Cd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Cd||{});function Wr(t,n,e,i){let r=U(null);try{let[o,s,a]=t.inputs[e],c=null;(s&Cd.SignalBased)!==0&&(c=n[o][Ue]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):D_(n,c,o,i)}finally{U(r)}}function Cb(t,n,e,i,r){let o=Gi(),s=i&2;try{Wi(-1),s&&n.length>$e&&bb(t,n,$e,!1);let a=s?he.TemplateUpdateStart:he.TemplateCreateStart;be(a,r,e),e(i,r)}finally{Wi(o);let a=s?he.TemplateUpdateEnd:he.TemplateCreateEnd;be(a,r,e)}}function Ed(t,n,e){jS(t,n,e),(e.flags&64)===64&&BS(t,n,e)}function ba(t,n,e=hn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function OS(t,n,e,i){let o=i.get(q_,W_)||e===Fn.ShadowDom||e===Fn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return FS(s),s}function FS(t){PS(t)}var PS=()=>null;function LS(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function Eb(t,n,e,i,r,o){let s=n[k];if(Hp(t,s,n,e,i)){yi(t)&&VS(n,t.index);return}t.type&3&&(e=LS(e)),wb(t,n,e,i,r,o)}function wb(t,n,e,i,r,o){if(t.type&3){let s=hn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function VS(t,n){let e=pn(n,t);e[H]&16||(e[H]|=64)}function jS(t,n,e){let i=e.directiveStart,r=e.directiveEnd;yi(e)&&kS(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||nd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=ua(n,t,s,e);if(Go(c,n),o!==null&&$S(n,s-i,c,a,e,o),Gn(a)){let l=pn(e.index,n);l[vt]=ua(n,t,s,e)}}}function BS(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Iy();try{Wi(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Fl(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&US(c,l)}}finally{Wi(-1),Fl(s)}}function US(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function jp(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];lb(n,o.selectors,!1)&&(i??=[],Gn(o)?i.unshift(o):i.push(o))}return i}function HS(t,n,e,i,r,o){let s=hn(t,n);zS(n[Se],s,o,t.value,e,i,r)}function zS(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Dl(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function $S(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Wr(i,e,c,l)}}function Bp(t,n,e,i,r){let o=$e+e,s=n[k],a=r(s,n,t,i,e);n[o]=a,Vo(t,!0);let c=t.type===2;return c?(rb(n[Se],a,t),(yy()===0||Po(t))&&Go(a,n),_y()):Go(a,n),Bl()&&(!c||!md(t))&&Op(s,n,a,t),t}function Up(t){let n=t;return _h()?bh():(n=n.parent,Vo(n,!1)),n}function GS(t,n){let e=t[mi];if(!e)return;let i;try{i=e.get(tn,null)}catch(r){i=null}i?.(n)}function Hp(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];Wr(f,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];Wr(d,l,i,r),a=!0}return a}function WS(t,n,e,i,r,o){let s=null,a=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let m=0;m<f.length;m+=2){let g=f[m];if(g>=a&&g<=c){let b=n.data[g],x=f[m+1];Wr(b,e[g],x,o),l=!0}else if(g>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(Wr(i,e[s],r,o),l=!0),l}function qS(t,n){let e=pn(n,t),i=e[k];YS(i,e);let r=e[An];r!==null&&e[ko]===null&&(e[ko]=Y_(r,e[mi])),be(he.ComponentStart);try{zp(i,e,e[vt])}finally{be(he.ComponentEnd,e[vt])}}function YS(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function zp(t,n,e){Ll(n);try{let i=t.viewQuery;i!==null&&qh(1,i,e);let r=t.template;r!==null&&Cb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[$n]?.finishViewCreation(t),t.staticContentQueries&&Z_(t,n),t.staticViewQueries&&qh(2,t.viewQuery,e);let o=t.components;o!==null&&ZS(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[H]&=-5,Vl()}}function ZS(t,n){for(let e=0;e<n.length;e++)qS(t,n[e])}function $p(t,n,e,i){let r=U(null);try{let o=n.tView,a=t[H]&4096?4096:16,c=Lp(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[Hi]=l;let d=t[$n];return d!==null&&(c[$n]=d.createEmbeddedView(o)),zp(o,c,e),c}finally{U(r)}}function rd(t,n){return!n||n.firstChild===null||B_(t)}function ha(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(fn(o)),un(o)&&Db(o,i);let s=e.type;if(s&8)ha(t,n,e.child,i);else if(s&32){let a=Tp(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=gb(n,e);if(Array.isArray(a))i.push(...a);else{let c=fi(n[Wt]);ha(c[k],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Db(t,n){for(let e=yt;e<t.length;e++){let i=t[e],r=i[k].firstChild;r!==null&&ha(i[k],i,r,n)}t[$i]!==t[An]&&n.push(t[$i])}function xb(t){if(t[Pr]!==null){for(let n of t[Pr])n.impl.addSequence(n);t[Pr].length=0}}var Ib=[];function KS(t){return t[en]??QS(t)}function QS(t){let n=Ib.pop()??Object.create(JS);return n.lView=t,n}function XS(t){t.lView[en]!==t&&(t.lView=null,Ib.push(t))}var JS=B(C({},ki),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{jr(t.lView)},consumerOnSignalRead(){this.lView[en]=this}});function eM(t){let n=t[en]??Object.create(tM);return n.lView=t,n}var tM=B(C({},ki),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=fi(t.lView);for(;n&&!Sb(n[k]);)n=fi(n);n&&uh(n)},consumerOnSignalRead(){this.lView[en]=this}});function Sb(t){return t.type!==2}function Mb(t){if(t[Li]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Li])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[H]&8192)}}var nM=100;function Tb(t,n=0){let i=t[Rn].rendererFactory,r=!1;r||i.begin?.();try{iM(t,n)}finally{r||i.end?.()}}function iM(t,n){let e=Ch();try{qs(!0),rp(t,n);let i=0;for(;na(t);){if(i===nM)throw new E(103,!1);i++,rp(t,1)}}finally{qs(e)}}function rM(t,n,e,i){if(Vr(n))return;let r=n[H],o=!1,s=!1;Ll(n);let a=!0,c=null,l=null;o||(Sb(t)?(l=KS(n),c=ci(l)):kc()===null?(a=!1,l=eM(n),c=ci(l)):n[en]&&(Pi(n[en]),n[en]=null));try{dh(n),wy(t.bindingStartIndex),e!==null&&Cb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let g=t.preOrderCheckHooks;g!==null&&Zl(n,g,null)}else{let g=t.preOrderHooks;g!==null&&Kl(n,g,0,null),Nh(n,0)}if(s||oM(n),Mb(n),Ab(n,0),t.contentQueries!==null&&Z_(t,n),!o)if(d){let g=t.contentCheckHooks;g!==null&&Zl(n,g)}else{let g=t.contentHooks;g!==null&&Kl(n,g,1),Nh(n,1)}aM(t,n);let f=t.components;f!==null&&Nb(n,f,0);let m=t.viewQuery;if(m!==null&&qh(2,m,i),!o)if(d){let g=t.viewCheckHooks;g!==null&&Zl(n,g)}else{let g=t.viewHooks;g!==null&&Kl(n,g,2),Nh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Tl]){for(let g of n[Tl])g();n[Tl]=null}o||(xb(n),n[H]&=-73)}catch(d){throw o||jr(n),d}finally{l!==null&&(Fi(l,c),a&&XS(l)),Vl()}}function Ab(t,n){for(let e=H_(t);e!==null;e=z_(e))for(let i=yt;i<e.length;i++){let r=e[i];Rb(r,n)}}function oM(t){for(let n=H_(t);n!==null;n=z_(n)){if(!(n[H]&2))continue;let e=n[Lr];for(let i=0;i<e.length;i++){let r=e[i];uh(r)}}}function sM(t,n,e){be(he.ComponentStart);let i=pn(n,t);try{Rb(i,e)}finally{be(he.ComponentEnd,i[vt])}}function Rb(t,n){Rl(t)&&rp(t,n)}function rp(t,n){let i=t[k],r=t[H],o=t[en],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&_o(o)),s||=!1,o&&(o.dirty=!1),t[H]&=-9217,s)rM(i,t,i.template,t[vt]);else if(r&8192){let a=U(null);try{Mb(t),Ab(t,1);let c=i.components;c!==null&&Nb(t,c,1),xb(t)}finally{U(a)}}}function Nb(t,n,e){for(let i=0;i<n.length;i++)sM(t,n[i],e)}function aM(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Wi(~r);else{let o=r,s=e[++i],a=e[++i];xy(s,o);let c=n[o];be(he.HostBindingsUpdateStart,c);try{a(2,c)}finally{be(he.HostBindingsUpdateEnd,c)}}}}finally{Wi(-1)}}function Gp(t,n){let e=Ch()?64:1088;for(t[Rn].changeDetectionScheduler?.notify(n);t;){t[H]|=e;let i=fi(t);if(Lo(t)&&!i)return t;t=i}return null}function kb(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function cM(t,n){let e=yt+n;if(e<t.length)return t[e]}function Wp(t,n,e,i=!0){let r=n[k];if(dM(r,n,t,e),i){let s=ip(e,t),a=n[Se],c=a.parentNode(t[$i]);c!==null&&CS(r,t[Dt],a,n,c,s)}let o=n[ko];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function lM(t,n){let e=od(t,n);return e!==void 0&&kp(e[k],e),e}function od(t,n){if(t.length<=yt)return;let e=yt+n,i=t[e];if(i){let r=i[Hi];r!==null&&r!==t&&Np(r,i),n>0&&(t[e-1][dn]=i[dn]);let o=Xs(t,yt+n);bS(i[k],i);let s=o[$n];s!==null&&s.detachView(o[k]),i[dt]=null,i[dn]=null,i[H]&=-129}return i}function dM(t,n,e,i){let r=yt+i,o=e.length;i>0&&(e[r-1][dn]=n),i<o-yt?(n[dn]=e[r],eh(e,yt+i,n)):(e.push(n),n[dn]=null),n[dt]=e;let s=n[Hi];s!==null&&e!==s&&Ob(s,n);let a=n[$n];a!==null&&a.insertView(t),Nl(n),n[H]|=128}function Ob(t,n){let e=t[Lr],i=n[dt];if(vi(i))t[H]|=2;else{let r=i[dt][Wt];n[Wt]!==r&&(t[H]|=2)}e===null?t[Lr]=[n]:e.push(n)}var Zi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[k];return ha(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[vt]}set context(n){this._lView[vt]=n}get destroyed(){return Vr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[dt];if(un(n)){let e=n[ta],i=e?e.indexOf(this):-1;i>-1&&(od(n,i),Xs(e,i))}this._attachedToViewContainer=!1}kp(this._lView[k],this._lView)}onDestroy(n){kl(this._lView,n)}markForCheck(){Gp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[H]&=-129}reattach(){Nl(this._lView),this._lView[H]|=128}detectChanges(){this._lView[H]|=1024,Tb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new E(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Lo(this._lView),e=this._lView[Hi];e!==null&&!n&&Np(e,this._lView),hb(this._lView[k],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new E(902,!1);this._appRef=n;let e=Lo(this._lView),i=this._lView[Hi];i!==null&&!e&&Ob(i,this._lView),Nl(this._lView)}};var gn=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=uM;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=$p(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Zi(o)}}return t})();function uM(){return wd(Ye(),Y())}function wd(t,n){return t.type&4?new gn(n,t,Yo(t,n)):null}function Zo(t,n,e,i,r){let o=t.data[n];if(o===null)o=fM(t,n,e,i,r),Dy()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=by();o.injectorIndex=s===null?-1:s.injectorIndex}return Vo(o,!0),o}function fM(t,n,e,i,r){let o=yh(),s=_h(),a=s?o:o&&o.parent,c=t.data[n]=pM(t,a,e,n,i,r);return hM(t,c,o,s),c}function hM(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function pM(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return mh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:xh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var mM=()=>null,gM=()=>null;function op(t,n){return mM(t,n)}function vM(t,n,e){return gM(t,n,e)}var Fb=class{},Ze=class{},Ge=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>yM()}return t})();function yM(){let t=Y(),n=Ye(),e=pn(n.index,t);return(vi(e)?e:t)[Se]}var Pb=(()=>{class t{static \u0275prov=F({token:t,providedIn:"root",factory:()=>null})}return t})();function Lb(t){return t.debugInfo?.className||t.type.name||null}var Xl={},sd=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Xl,i);return r!==Xl||e===Xl?r:this.parentInjector.get(n,e,i)}};function qp(t){return jb(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function Vb(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function jb(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function _M(t,n,e){return t[n]=e}function xi(t,n,e){if(e===vn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function zo(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&gI(r,o);let s=yi(t)?pn(t.index,n):n;Gp(s,5);let a=n[vt],c=s_(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=s_(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function s_(t,n,e,i){let r=U(null);try{return be(he.OutputStart,n,e),e(i)!==!1}catch(o){return GS(t,o),!1}finally{be(he.OutputEnd,n,e),U(r)}}function Bb(t,n,e,i,r,o,s,a){let c=Po(t),l=!1,d=null;if(!i&&c&&(d=CM(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=hn(t,e),m=i?i(f):f;yI(e,m,o,a),i||(a.__ngNativeEl__=f);let g=r.listen(m,o,a);if(!bM(o)){let b=i?x=>i(fn(x[t.index])):t.index;Ub(b,n,e,o,a,g,!1)}}return l}function bM(t){return t.startsWith("animation")||t.startsWith("transition")}function CM(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[Oo],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function Ub(t,n,e,i,r,o,s){let a=n.firstCreatePass?hh(n):null,c=fh(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function a_(t,n,e,i,r){let o=null,s=null,a=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let d=t.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let m=d[f];if(m>=s&&m<=a)c=!0,ad(t,n,m,d[f+1],i,r);else if(m>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,ad(t,n,o,i,i,r)),c}function ad(t,n,e,i,r,o){let s=n[e],a=n[k],l=a.data[e].outputs[i],f=s[l].subscribe(o);Ub(t.index,a,n,r,o,f,!0)}function Oe(){EM()}function EM(){let t=Y(),n=ke(),e=Ye();if(n.firstCreatePass&&DM(n,e),e.controlDirectiveIndex===-1)return;Ki("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new cd(t,n,e))}function Fe(){wM()}function wM(){let t=Y(),n=ke(),e=jo();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new cd(t,n,e))}var cd=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return hn(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];a_(this.tNode,this.lView,i,n,zo(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];a_(this.tNode,this.lView,i,e,zo(this.tNode,this.lView,n))}listenToDom(n,e){Bb(this.tNode,this.tView,this.lView,void 0,this.lView[Se],n,e,zo(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];Wr(a,c,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];Wr(l,d,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";WS(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=c_(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=c_(o.directive);s!==null&&i.push(...s)}}}return e}};function c_(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function DM(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}xM(t,n)}function xM(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(l_(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(l_(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[m,g,b]=f;if(c>=g&&c<=b)return n.flags|=r,n.customControlIndex=m,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function l_(t,n){return IM(t,n)&&SM(t,n+"Change")}function IM(t,n){return n in t.inputs}function SM(t,n){return n in t.outputs}var sp=Symbol("BINDING");var Yr=new _("");function ld(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Cl(r,a);else if(o==2){let c=a,l=n[++s];i=Cl(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function w(t,n=0){let e=Y();if(e===null)return M(t,n);let i=Ye();return P_(i,e,gt(t),n)}function Hb(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}AM(t,n,e,a,o,c,l)}o!==null&&i!==null&&MM(e,i,o)}function MM(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new E(-301,!1);i.push(n[r],o)}}function TM(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function AM(t,n,e,i,r,o,s){let a=i.length,c=null;for(let m=0;m<a;m++){let g=i[m];c===null&&Gn(g)&&(c=g,TM(t,e,m)),zh(nd(e,n),t,g.type)}PM(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let g=i[m];g.providersResolver&&g.providersResolver(g)}let l=!1,d=!1,f=_b(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let m=0;m<a;m++){let g=i[m];if(e.mergedAttrs=$o(e.mergedAttrs,g.hostAttrs),NM(t,e,n,f,g),FM(f,g,r),s!==null&&s.has(g)){let[x,R]=s.get(g);e.directiveToIndex.set(g.type,[f,x+e.directiveStart,R+e.directiveStart])}else(o===null||!o.has(g))&&e.directiveToIndex.set(g.type,f);g.contentQueries!==null&&(e.flags|=4),(g.hostBindings!==null||g.hostAttrs!==null||g.hostVars!==0)&&(e.flags|=64);let b=g.type.prototype;!l&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}RM(t,e,o)}function RM(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))d_(0,n,r,i),d_(1,n,r,i),f_(n,i,!1);else{let o=e.get(r);u_(0,n,o,i),u_(1,n,o,i),f_(n,i,!0)}}}function d_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),zb(n,o)}}function u_(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),zb(n,s)}}function zb(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function f_(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Mp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function NM(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Ar(r.type,!0)),s=new $r(o,Gn(r),w,null);t.blueprint[i]=s,e[i]=s,kM(t,n,i,_b(t,e,r.hostVars,vn),r)}function kM(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;OM(s)!=a&&s.push(a),s.push(e,i,o)}}function OM(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function FM(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Gn(n)&&(e[""]=t)}}function PM(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function Yp(t,n,e,i,r,o,s,a){let c=n[k],l=c.consts,d=Wn(l,s),f=Zo(c,t,e,i,d);return o&&Hb(c,n,f,Wn(l,a),r),f.mergedAttrs=$o(f.mergedAttrs,f.attrs),f.attrs!==null&&ld(f,f.attrs,!1),f.mergedAttrs!==null&&ld(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Zp(t,n){S_(t,n),ah(n)&&t.queries.elementEnd(n)}function LM(t,n,e,i,r,o){let s=n.consts,a=Wn(s,r),c=Zo(n,t,e,i,a);if(c.mergedAttrs=$o(c.mergedAttrs,c.attrs),o!=null){let l=Wn(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&ld(c,c.attrs,!1),c.mergedAttrs!==null&&ld(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var $b=typeof ShadowRoot<"u",VM=typeof Document<"u";function jM(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Cd.SignalBased)!==0};return r&&(o.transform=r),o})}function BM(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function UM(t,n,e){let i=n instanceof Ee?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new sd(e,i):e}function HM(t){let n=t.get(Ze,null);if(n===null)throw new E(407,!1);let e=t.get(Pb,null),i=t.get(zn,null),r=t.get(Kn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function zM(t,n){let e=Gb(t);return nb(n,e,e==="svg"?ch:e==="math"?uy:null)}function $M(t){if(t?.toLowerCase()==="script")throw new E(905,!1)}function Gb(t){return(t.selectors[0][0]||"div").toLowerCase()}var Wo=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=jM(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=BM(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=aS(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){be(he.DynamicComponentStart);let a=U(null);try{let c=this.componentDef,l=UM(c,r||this.ngModule,n),d=HM(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(Lb(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{U(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=GM(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?OS(l,r,a.encapsulation,e):zM(a,l);$M(d?.tagName);let f=e.get(Yr,null),m=WM(d,()=>e.get(V,null)??G_());f&&f.addHost(m);let g=s?.some(h_)||o?.some(R=>typeof R!="function"&&R.bindings.some(h_)),b=Lp(null,c,null,512|yb(a),null,null,n,l,e,null,Y_(d,e,!0));f&&$b&&m instanceof ShadowRoot&&kl(b,()=>{f.removeHost(m)}),b[$e]=d,Ll(b);let x=null;try{let R=Yp($e,b,2,"#host",()=>c.directiveRegistry,!0,0);rb(l,d,R),Go(d,b),Ed(c,b,R),bp(c,R,b),Zp(c,R),i!==void 0&&YM(R,this.ngContentSelectors,i),x=pn(R.index,b),b[vt]=x[vt],zp(c,b,null)}catch(R){throw x!==null&&Gh(x),Gh(b),R}finally{be(he.DynamicComponentEnd),Vl()}return new dd(this.componentType,b,!!g)}};function GM(t,n,e,i){let r=t?["ng-version","22.0.5"]:cS(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[sp].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let m of f.bindings){a+=m[sp].requiredVars;let g=d+1;m.create&&(m.targetIdx=g,(o??=[]).push(m)),m.update&&(m.targetIdx=g,(s??=[]).push(m))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,m=Qf(f);c.push(m)}return Pp(0,null,qM(o,s),1,a,c,null,null,null,[r],null)}function WM(t,n){let e=t.getRootNode?.();return VM&&e instanceof Document?e.head:e&&$b&&e instanceof ShadowRoot?e:n().head}function qM(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function h_(t){let n=t[sp].kind;return n==="input"||n==="twoWay"}var dd=class extends Fb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Al(e[k],$e),this.location=Yo(this._tNode,e),this.instance=pn(this._tNode.index,e)[vt],this.hostView=this.changeDetectorRef=new Zi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Hp(i,r[k],r,n,e);this.previousInputValues.set(n,e);let s=pn(i.index,r);Gp(s,1)}get injector(){return new Yi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function YM(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var qt=(()=>{class t{static __NG_ELEMENT_ID__=ZM}return t})();function ZM(){let t=Ye();return Wb(t,Y())}var ap=class t extends qt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Yo(this._hostTNode,this._hostLView)}get injector(){return new Yi(this._hostTNode,this._hostLView)}get parentInjector(){let n=vp(this._hostTNode,this._hostLView);if(A_(n)){let e=td(n,this._hostLView),i=ed(n),r=e[k].data[i+8];return new Yi(r,e)}else return new Yi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=p_(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-yt}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=op(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,rd(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new Wo(Vi(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let ee=this.parentInjector.get(Ee,null);ee&&(o=ee)}let m=Vi(d.componentType??{}),g=op(this._lContainer,m?.id??null),b=g?.firstChild??null,x=d.create(f,r,b,o,s,a);return this.insertImpl(x.hostView,c,rd(this._hostTNode,g)),x}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(py(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[dt],l=new t(c,c[Dt],c[dt]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Wp(s,r,o,i),n.attachToViewContainerRef(),eh(Fh(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=p_(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=od(this._lContainer,e);i&&(Xs(Fh(this._lContainer),e),kp(i[k],i))}detach(n){let e=this._adjustIndex(n,-1),i=od(this._lContainer,e);return i&&Xs(Fh(this._lContainer),e)!=null?new Zi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function p_(t){return t[ta]}function Fh(t){return t[ta]||(t[ta]=[])}function Wb(t,n){let e,i=n[t.index];return un(i)?e=i:(e=kb(i,n,null,t),n[t.index]=e,Vp(n,e)),QM(e,n,t,i),new ap(e,t,n)}function KM(t,n){let e=t[Se],i=e.createComment(""),r=hn(n,t),o=e.parentNode(r);return id(e,o,i,e.nextSibling(r),!1),i}var QM=eT,XM=()=>!1;function JM(t,n,e){return XM(t,n,e)}function eT(t,n,e,i){if(t[$i])return;let r;e.type&8?r=fn(i):r=KM(n,e),t[$i]=r}var cp=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},lp=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Qp(n,e).matches!==null&&this.queries[e].setDirty()}},ud=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=oT(n):this.predicate=n}},dp=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},up=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,tT(e,o)),this.matchTNodeWithReadOption(n,e,Ql(e,n,o,!1,!1))}else i===gn?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Ql(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===z||r===qt||r===gn&&e.type&4)this.addMatch(e.index,-2);else{let o=Ql(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function tT(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function nT(t,n){return t.type&11?Yo(t,n):t.type&4?wd(t,n):null}function iT(t,n,e,i){return e===-1?nT(n,t):e===-2?rT(t,n,i):ua(t,t[k],e,n)}function rT(t,n,e){if(e===z)return Yo(n,t);if(e===gn)return wd(n,t);if(e===qt)return Wb(n,t)}function qb(t,n,e,i){let r=n[$n].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(iT(n,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function fp(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=qb(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=yt;f<d.length;f++){let m=d[f];m[Hi]===m[dt]&&fp(m[k],m,l,i)}if(d[Lr]!==null){let f=d[Lr];for(let m=0;m<f.length;m++){let g=f[m];fp(g[k],g,l,i)}}}}}return i}function Kp(t,n){return t[$n].queries[n].queryList}function Yb(t,n,e){let i=new bi((e&4)===4);return vy(t,n,i,i.destroy),(n[$n]??=new lp).queries.push(new cp(i))-1}function Zb(t,n,e){let i=ke();return i.firstCreatePass&&(Qb(i,new ud(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),Yb(i,Y(),n)}function Kb(t,n,e,i){let r=ke();if(r.firstCreatePass){let o=Ye();Qb(r,new ud(n,e,i),o.index),sT(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return Yb(r,Y(),e)}function oT(t){return t.split(",").map(n=>n.trim())}function Qb(t,n,e){t.queries===null&&(t.queries=new dp),t.queries.track(new up(n,e))}function sT(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Qp(t,n){return t.queries.getByIndex(n)}function Xb(t,n){let e=t[k],i=Qp(e,n);return i.crossesNgTemplate?fp(e,t,n,[]):qb(e,t,i,n)}function Jb(t,n,e){let i,r=Fs(()=>{i._dirtyCounter();let o=aT(i,t);if(n&&o===void 0)throw new E(-951,!1);return o});return i=r[Ue],i._dirtyCounter=re(0),i._flatValue=void 0,r}function Xp(t){return Jb(!0,!1,t)}function Jp(t){return Jb(!0,!0,t)}function e0(t,n){let e=t[Ue];e._lView=Y(),e._queryIndex=n,e._queryList=Kp(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function aT(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[H]&4)return n?void 0:wt;let r=Kp(e,i),o=Xb(e,i);return r.reset(o,j_),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function Qi(t){return!!t&&typeof t.then=="function"}function em(t){return!!t&&typeof t.subscribe=="function"}var Ei=class{},Dd=class{};var fd=class extends Ei{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Zv(n);this._bootstrapComponents=KI(o.bootstrap),this._r3Injector=Ih(n,e,[{provide:Ei,useValue:this},...i],Ks(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},hd=class extends Dd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new fd(this.moduleType,n,[])}};var pa=class extends Ei{injector;instance=null;constructor(n){super();let e=new Nr([...n.providers,{provide:Ei,useValue:this}],n.parent||No(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ca(t,n,e=null){return new pa({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var cT=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=nh(!1,e.type),r=i.length>0?Ca([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=F({token:t,providedIn:"environment",factory:()=>new t(M(Ee))})}return t})();function N(t){return ga(()=>{let n=t0(t),e=B(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==yp.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(cT).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Fn.Emulated,styles:t.styles||wt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Ki("NgStandalone"),n0(e);let i=t.dependencies;return e.directiveDefs=m_(i,lT),e.pipeDefs=m_(i,Kv),e.id=fT(e),e})}function lT(t){return Vi(t)||Qf(t)}function G(t){return ga(()=>({type:t.type,bootstrap:t.bootstrap||wt,declarations:t.declarations||wt,imports:t.imports||wt,exports:t.exports||wt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function dT(t,n){if(t==null)return Bi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=Cd.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function uT(t){if(t==null)return Bi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function P(t){return ga(()=>{let n=t0(t);return n0(n),n})}function t0(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Bi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||wt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:dT(t.inputs,n),outputs:uT(t.outputs),debugInfo:null}}function n0(t){t.features?.forEach(n=>n(t))}function m_(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function fT(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var i0=new _("");var tm=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(i0,{optional:!0})??[];injector=u(ce);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ut(this.injector,r);if(Qi(o))e.push(o);else if(em(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function nm(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function hT(t){return Object.getPrototypeOf(t.prototype).constructor}function Qe(t){let n=hT(t.type),e=!0,i=[t];for(;n;){let r;if(Gn(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new E(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=Ph(t.inputs),s.declaredInputs=Ph(t.declaredInputs),s.outputs=Ph(t.outputs);let a=r.hostBindings;a&&yT(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&gT(t,c),l&&vT(t,l),pT(t,r),Yv(t.outputs,r.outputs),Gn(r)&&r.data.animation){let d=t.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===Qe&&(e=!1)}}n=Object.getPrototypeOf(n)}mT(i)}function pT(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function mT(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=$o(r.hostAttrs,e=$o(e,r.hostAttrs))}}function Ph(t){return t===Bi?{}:t===wt?[]:t}function gT(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function vT(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function yT(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function r0(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=$o(t.mergedAttrs,t.attrs);let d=t.tView=Pp(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Vo(t,!1);let c=bT(e,n,t,i);Bl()&&Op(e,n,c,t),Go(c,n);let l=kb(c,n,c,t);n[i+$e]=l,Vp(n,l),JM(l,t,n)}function _T(t,n,e,i,r,o,s,a,c,l,d){let f=e+$e,m;return n.firstCreatePass?(m=Zo(n,f,4,s||null,a||null),Ol()&&Hb(n,t,m,Wn(n.consts,l),jp),S_(n,m)):m=n.data[f],r0(m,t,n,e,i,r,o,c),Po(m)&&Ed(n,t,m),l!=null&&ba(t,m,d),m}function im(t,n,e,i,r,o,s,a,c,l,d){let f=e+$e,m;if(n.firstCreatePass){if(m=Zo(n,f,4,s||null,a||null),l!=null){let g=Wn(n.consts,l);m.localNames=[];for(let b=0;b<g.length;b+=2)m.localNames.push(g[b],-1)}}else m=n.data[f];return r0(m,t,n,e,i,r,o,c),l!=null&&ba(t,m,d),m}function W(t,n,e,i,r,o,s,a){let c=Y(),l=ke(),d=Wn(l.consts,o);return _T(c,l,t,n,e,i,r,d,void 0,s,a),W}var bT=CT;function CT(t,n,e,i){return ra(!0),n[Se].createComment("")}var xd=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var rm=new _("");var Ea=new _("");function o0(){ff(()=>{let t="";throw new E(600,t)})}var ET=10;var nn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(tn);afterRenderManager=u(yd);zonelessEnabled=u(sa);rootEffectScheduler=u(zl);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new S;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(_i);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(X(e=>!e))}constructor(){u(Kn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(Ee);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=ce.NULL){return this._injector.get(O).run(()=>{if(be(he.BootstrapComponentStart),!this._injector.get(tm).done){let ee="";throw new E(405,ee)}let a=Vi(e),c=this._injector.get(Ei),l=new Wo(a,c);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:m}=wT(i),g=d||l.selector,b=l.create(r,[],g,c.injector,f,m),x=b.location.nativeElement,R=b.injector.get(rm,null);return R?.registerApplication(x),b.onDestroy(()=>{this.detachView(b.hostView),da(this.components,b),R?.unregisterApplication(x)}),this._loadComponent(b),be(he.BootstrapComponentEnd,b),b})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){be(he.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(vd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw be(he.ChangeDetectionEnd),new E(101,!1);let e=U(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,U(e),this.afterTick.next(),be(he.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ze,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<ET;){be(he.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{be(he.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!na(r))continue;let o=i&&!this.zonelessEnabled?0:1;Tb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>na(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;da(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Ea,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>da(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new E(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function wT(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function da(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function pe(t,n,e,i){let r=Y(),o=Br();if(xi(r,o,n)){let s=ke(),a=jo();HS(a,r,t,n,e,i)}return pe}function we(t,n,e,i,r,o,s,a){Ki("NgControlFlow");let c=Y(),l=ke(),d=Wn(l.consts,o);return im(c,l,t,n,e,i,r,d,256,s,a),om}function om(t,n,e,i,r,o,s,a){Ki("NgControlFlow");let c=Y(),l=ke(),d=Wn(l.consts,o);return im(c,l,t,n,e,i,r,d,512,s,a),om}function De(t,n){Ki("NgControlFlow");let e=Y(),i=Br(),r=e[i]!==vn?e[i]:-1,o=r!==-1?g_(e,$e+r):void 0,s=0;if(xi(e,i,t)){let a=U(null);try{if(o!==void 0&&lM(o,s),t!==-1){let c=$e+t,l=g_(e,c),d=DT(e[k],c),f=vM(l,d,e),m=$p(e,d,n,{dehydratedView:f});Wp(l,m,s,rd(d,f))}}finally{U(a)}}else if(o!==void 0){let a=cM(o,s);a!==void 0&&(a[vt]=n)}}function g_(t,n){return t[n]}function DT(t,n){return Al(t,n)}function D(t,n,e){let i=Y(),r=Br();if(xi(i,r,n)){let o=ke(),s=jo();Eb(s,i,t,n,i[Se],e)}return D}function hp(t,n,e,i,r){Hp(n,t,e,r?"class":"style",i)}function p(t,n,e,i){let r=Y(),o=r[k],s=t+$e,a=o.firstCreatePass?Yp(s,r,2,n,jp,Ol(),e,i):o.data[s];if(yi(a)){let c=r[Rn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Lb(l),()=>(v_(t,n,r,a,i),p))}}return v_(t,n,r,a,i),p}function v_(t,n,e,i,r){if(Bp(i,e,t,n,s0),Po(i)){let o=e[k];Ed(o,e,i),bp(o,i,e)}r!=null&&ba(e,i)}function h(){let t=ke(),n=Ye(),e=Up(n);return t.firstCreatePass&&Zp(t,e),gh(e)&&vh(),ph(),e.classesWithoutHost!=null&&Xx(e)&&hp(t,e,Y(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Jx(e)&&hp(t,e,Y(),e.stylesWithoutHost,!1),h}function le(t,n,e,i){return p(t,n,e,i),h(),le}function jt(t,n,e,i){let r=Y(),o=r[k],s=t+$e,a=o.firstCreatePass?LM(s,o,2,n,e,i):o.data[s];return Bp(a,r,t,n,s0),i!=null&&ba(r,a),jt}function Bt(){let t=Ye(),n=Up(t);return gh(n)&&vh(),ph(),Bt}function yn(t,n,e,i){return jt(t,n,e,i),Bt(),yn}var s0=(t,n,e,i,r)=>(ra(!0),nb(n[Se],i,xh()));function sm(t,n,e){let i=Y(),r=i[k],o=t+$e,s=r.firstCreatePass?Yp(o,i,8,"ng-container",jp,Ol(),n,e):r.data[o];if(Bp(s,i,t,"ng-container",xT),Po(s)){let a=i[k];Ed(a,i,s),bp(a,s,i)}return e!=null&&ba(i,s),sm}function am(){let t=ke(),n=Ye(),e=Up(n);return t.firstCreatePass&&Zp(t,e),am}function Ko(t,n,e){return sm(t,n,e),am(),Ko}var xT=(t,n,e,i,r)=>(ra(!0),$I(n[Se],""));function Xe(){return Y()}function Xn(t,n,e){let i=Y(),r=Br();if(xi(i,r,n)){let o=ke(),s=jo();wb(s,i,t,n,i[Se],e)}return Xn}var wa="en-US";var IT=wa;function a0(t){typeof t=="string"&&(IT=t.toLowerCase().replace(/_/g,"-"))}function j(t,n,e){let i=Y(),r=ke(),o=Ye();return c0(r,i,i[Se],o,t,n,e),j}function c0(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=zo(i,n,o),Bb(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let m=d[f],g=d[f+1];c??=zo(i,n,o),ad(i,n,m,g,r,c)}if(l&&l.length)for(let f of l)c??=zo(i,n,o),ad(i,n,f,r,r,c)}}function I(t=1){return Ry(t)}function ST(t,n){let e=null,i=nS(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?lb(t,o,!0):oS(i,o))return r}return e}function Je(t){let n=Y()[Wt][Dt];if(!n.projection){let e=t?t.length:1,i=n.projection=ny(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?ST(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function de(t,n=0,e,i,r,o){let s=Y(),a=ke(),c=i?t+1:null;c!==null&&im(s,a,c,i,r,o,null,e);let l=Zo(a,$e+t,16,null,e||null);l.projection===null&&(l.projection=n),bh();let f=!s[ko]||mh();s[Wt][Dt].projection[l.projection]===null&&c!==null?MT(s,a,c):f&&!md(l)&&MS(a,s,l)}function MT(t,n,e){let i=$e+e,r=n.data[i],o=t[i],s=op(o,r.tView.ssrId),a=$p(t,r,void 0,{dehydratedView:s});Wp(o,a,0,rd(r,s))}function Jn(t,n,e,i){return Kb(t,n,e,i),Jn}function Yt(t,n,e){return Zb(t,n,e),Yt}function oe(t){let n=Y(),e=ke(),i=Pl();ia(i+1);let r=Qp(e,i);if(t.dirty&&hy(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Xb(n,i);t.reset(o,j_),t.notifyOnChanges()}return!0}return!1}function se(){return Kp(Y(),Pl())}function Id(t,n,e,i,r){return e0(n,Kb(t,e,i,r)),Id}function Sd(t,n,e,i){return e0(t,Zb(n,e,i)),Sd}function Md(t=1){ia(Pl()+t)}function Xi(t){let n=Cy();return fy(n,$e+t)}function Yl(t,n){return t<<17|n<<2}function qr(t){return t>>17&32767}function TT(t){return(t&2)==2}function AT(t,n){return t&131071|n<<17}function pp(t){return t|2}function qo(t){return(t&131068)>>2}function Lh(t,n){return t&-131069|n<<2}function RT(t){return(t&1)===1}function mp(t){return t|1}function NT(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=qr(s),c=qo(s);t[i]=e;let l=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||Ro(f,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let m=qr(t[a+1]);t[i+1]=Yl(m,a),m!==0&&(t[m+1]=Lh(t[m+1],i)),t[a+1]=AT(t[a+1],i)}else t[i+1]=Yl(a,0),a!==0&&(t[a+1]=Lh(t[a+1],i)),a=i;else t[i+1]=Yl(c,0),a===0?a=i:t[c+1]=Lh(t[c+1],i),c=i;l&&(t[i+1]=pp(t[i+1])),y_(t,d,i,!0),y_(t,d,i,!1),kT(n,d,t,i,o),s=Yl(a,c),o?n.classBindings=s:n.styleBindings=s}function kT(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Ro(o,n)>=0&&(e[i+1]=mp(e[i+1]))}function y_(t,n,e,i){let r=t[e+1],o=n===null,s=i?qr(r):qo(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];OT(c,n)&&(a=!0,t[s+1]=i?mp(l):pp(l)),s=i?qr(l):qo(l)}a&&(t[e+1]=i?pp(r):mp(r))}function OT(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Ro(t,n)>=0:!1}var On={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function FT(t){return t.substring(On.key,On.keyEnd)}function PT(t){return LT(t),l0(t,d0(t,0,On.textEnd))}function l0(t,n){let e=On.textEnd;return e===n?-1:(n=On.keyEnd=VT(t,On.key=n,e),d0(t,n,e))}function LT(t){On.key=0,On.keyEnd=0,On.value=0,On.valueEnd=0,On.textEnd=t.length}function d0(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function VT(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Zr(t,n,e){return u0(t,n,e,!1),Zr}function ae(t,n){return u0(t,n,null,!0),ae}function Ln(t){BT(WT,jT,t,!0)}function jT(t,n){for(let e=PT(n);e>=0;e=l0(n,e))Sl(t,FT(n),!0)}function u0(t,n,e,i){let r=Y(),o=ke(),s=Eh(2);if(o.firstUpdatePass&&h0(o,t,s,i),n!==vn&&xi(r,s,n)){let a=o.data[Gi()];p0(o,a,r,r[Se],t,r[s+1]=YT(n,e),i,s)}}function BT(t,n,e,i){let r=ke(),o=Eh(2);r.firstUpdatePass&&h0(r,null,o,i);let s=Y();if(e!==vn&&xi(s,o,e)){let a=r.data[Gi()];if(m0(a,i)&&!f0(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=Cl(c,e||"")),hp(r,a,s,e,i)}else qT(r,a,s,s[Se],s[o+1],s[o+1]=GT(t,n,e),i,o)}}function f0(t,n){return n>=t.expandoStartIndex}function h0(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Gi()],s=f0(t,e);m0(o,i)&&n===null&&!s&&(n=!1),n=UT(r,o,n,i),NT(r,o,n,e,s,i)}}function UT(t,n,e,i){let r=Sy(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Vh(null,t,n,e,i),e=ma(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Vh(r,t,n,e,i),o===null){let c=HT(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Vh(null,t,n,c[1],i),c=ma(c,n.attrs,i),zT(t,n,i,c))}else o=$T(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function HT(t,n,e){let i=e?n.classBindings:n.styleBindings;if(qo(i)!==0)return t[qr(i)]}function zT(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[qr(r)]=i}function $T(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=ma(i,s,e)}return ma(i,n.attrs,e)}function Vh(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=ma(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function ma(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Sl(t,s,e?!0:n[++o]))}return t===void 0?null:t}function GT(t,n,e){if(e==null||e==="")return wt;let i=[],r=Pn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function WT(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Sl(t,i,e)}function qT(t,n,e,i,r,o,s,a){r===vn&&(r=wt);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let m=c<r.length?r[c+1]:void 0,g=l<o.length?o[l+1]:void 0,b=null,x;d===f?(c+=2,l+=2,m!==g&&(b=f,x=g)):f===null||d!==null&&d<f?(c+=2,b=d):(l+=2,b=f,x=g),b!==null&&p0(t,n,e,i,b,x,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function p0(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],d=RT(l)?__(c,n,e,r,qo(l),s):void 0;if(!pd(d)){pd(o)||TT(l)&&(o=__(c,null,e,r,a,s));let f=lh(Gi(),e);AS(i,s,f,r,o)}}function __(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,m=e[r+1];m===vn&&(m=f?wt:void 0);let g=f?Ml(m,i):d===i?m:void 0;if(l&&!pd(g)&&(g=Ml(c,i)),pd(g)&&(a=g,s))return a;let b=t[r+1];r=s?qr(b):qo(b)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=Ml(c,i))}return a}function pd(t){return t!==void 0}function YT(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Ks(Pn(t)))),t}function m0(t,n){return(t.flags&(n?8:16))!==0}function y(t,n=""){let e=Y(),i=ke(),r=t+$e,o=i.firstCreatePass?Zo(i,r,1,n,null):i.data[r],s=ZT(i,e,o,n);e[r]=s,Bl()&&Op(i,e,s,o),Vo(o,!1)}var ZT=(t,n,e,i)=>(ra(!0),HI(n[Se],i));function KT(t,n,e,i=""){return xi(t,Br(),e)?n+Dl(e)+i:vn}function at(t){return xe("",t),at}function xe(t,n,e){let i=Y(),r=KT(i,t,n,e);return r!==vn&&QT(i,Gi(),r),xe}function QT(t,n,e){let i=lh(n,t);zI(t[Se],i,e)}function Ae(t,n,e){$l(n)&&(n=n());let i=Y(),r=Br();if(xi(i,r,n)){let o=ke(),s=jo();Eb(s,i,t,n,i[Se],e)}return Ae}function Pe(t,n){let e=$l(t);return e&&t.set(n),e}function Re(t,n){let e=Y(),i=ke(),r=Ye();return c0(i,e,e[Se],r,t,n),Re}function b_(t,n,e){let i=ke();i.firstCreatePass&&g0(n,i.data,i.blueprint,Gn(t),e)}function g0(t,n,e,i,r){if(t=gt(t),Array.isArray(t))for(let o=0;o<t.length;o++)g0(t[o],n,e,i,r);else{let o=ke(),s=Y(),a=Ye(),c=Rr(t)?t:gt(t.provide),l=rh(t),d=a.providerIndexes&1048575,f=a.directiveStart,m=a.providerIndexes>>20;if(Rr(t)||!t.multi){let g=new $r(l,r,w,null),b=Bh(c,n,r?d:d+m,f);b===-1?(zh(nd(a,s),o,c),jh(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(g),s.push(g)):(e[b]=g,s[b]=g)}else{let g=Bh(c,n,d+m,f),b=Bh(c,n,d,d+m),x=g>=0&&e[g],R=b>=0&&e[b];if(r&&!R||!r&&!x){zh(nd(a,s),o,c);let ee=eA(r?JT:XT,e.length,r,i,l,t);!r&&R&&(e[b].providerFactory=ee),jh(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(ee),s.push(ee)}else{let ee=v0(e[r?b:g],l,!r&&i);jh(o,t,g>-1?g:b,ee)}!r&&i&&R&&e[b].componentProviders++}}}function jh(t,n,e,i){let r=Rr(n),o=cy(n);if(r||o){let c=(o?gt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function v0(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Bh(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function XT(t,n,e,i,r){return gp(this.multi,[])}function JT(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=ua(i,i[k],this.providerFactory.index,r);s=c.slice(0,a),gp(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],gp(o,s);return s}function gp(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function eA(t,n,e,i,r,o){let s=new $r(t,e,w,null);return s.multi=[],s.index=n,s.componentProviders=0,v0(s,r,i&&!e),s}function et(t,n){return e=>{e.providersResolver=(i,r)=>b_(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>b_(i,r?r(n):n,!0))}}function Kr(t,n,e){return nA(Y(),Ey(),t,n,e)}function tA(t,n){let e=t[n];return e===vn?void 0:e}function nA(t,n,e,i,r,o){let s=n+e;return xi(t,s,r)?_M(t,s+1,o?i.call(o,r):i(r)):tA(t,s+1)}function Da(t,n){return wd(t,n)}var y0=(()=>{class t{applicationErrorHandler=u(tn);appRef=u(nn);taskService=u(_i);ngZone=u(O);zonelessEnabled=u(sa);tracing=u(Kn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ue;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ys):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(Rh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Fy:Sh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ys+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function _0(){return[{provide:zn,useExisting:y0},{provide:O,useClass:Zs},{provide:sa,useValue:!0}]}var cm=(()=>{class t{compileModuleSync(e){return new hd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function iA(){return typeof $localize<"u"&&$localize.locale||wa}var Td=new _("",{factory:()=>u(Td,{optional:!0,skipSelf:!0})||iA()});function Ut(t,n){return Fs(t,n?.equal)}function fe(t){return mv(t)}var rA=t=>t;function lm(t,n){if(typeof t=="function"){let e=vf(t,rA,n?.equal);return b0(e,n?.debugName)}else{let e=vf(t.source,t.computation,t.equal);return b0(e,t.debugName)}}function b0(t,n){let e=t[Ue],i=t;return i.set=r=>hv(e,r),i.update=r=>pv(e,r),i.asReadonly=Ul.bind(t),i}var T0=Symbol("InputSignalNode#UNSET"),EA=B(C({},Ps),{transformFn:void 0,applyValueToInputSignal(t,n){_r(t,n)}});function A0(t,n){let e=Object.create(EA);e.value=t,e.transformFn=n?.transform;function i(){if(Oi(e),e.value===T0){let r=null;throw new E(-950,r)}return e.value}return i[Ue]=e,i}var Ji=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>va(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},R0=(()=>{let t=new _("");return t.__NG_ELEMENT_ID__=n=>{let e=Ye();if(e===null)throw new E(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new E(-204,!1)},t})();function vm(t){return wA(t)?t.default:t}function wA(t){return t&&typeof t=="object"&&"default"in t}function C0(t,n){return A0(t,n)}function DA(t){return A0(T0,t)}var Qo=(C0.required=DA,C0);function E0(t,n){return Xp(n)}function xA(t,n){return Jp(n)}var Ia=(E0.required=xA,E0);function w0(t,n){return Xp(n)}function IA(t,n){return Jp(n)}var N0=(w0.required=IA,w0);var SA=1e4;var GG=SA-1e3;var um=class{supports(n){return qp(n)}create(n){return new fm(n)}},MA=(t,n)=>n,fm=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||MA}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<D0(i,r,o)?e:i,a=D0(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let m=0;m<l;m++){let g=m<o.length?o[m]:o[m]=0,b=g+m;d<=b&&b<l&&(o[m]=g+1)}let f=s.previousIndex;o[f]=d-l}}a!==c&&n(s,a,c)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!qp(n))throw new E(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,Vb(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new hm(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Rd),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Rd),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},hm=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},pm=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Rd=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new pm,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function D0(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function x0(){return new Nd([new um])}var Nd=(()=>{class t{factories;static \u0275prov=F({token:t,providedIn:"root",factory:x0});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=u(t,{optional:!0,skipSelf:!0});return t.create(e,i||x0())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new E(901,!1)}}return t})();var me=(()=>{class t{static __NG_ELEMENT_ID__=TA}return t})();function TA(t){return AA(Ye(),Y(),(t&16)===16)}function AA(t,n,e){if(yi(t)&&!e){let i=pn(t.index,n);return new Zi(i,i)}else if(t.type&175){let i=n[Wt];return new Zi(i,n)}return null}var mm=new _(""),RA=new _("");function xa(t){return!t.moduleRef}function NA(t){let n=xa(t)?t.r3Injector:t.moduleRef.injector,e=n.get(O);return e.run(()=>{xa(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(tn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),xa(t)){let o=()=>n.destroy(),s=t.platformInjector.get(mm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(mm);s.add(o),t.moduleRef.onDestroy(()=>{da(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return OA(i,e,()=>{let o=n.get(_i),s=o.add(),a=n.get(tm);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(Td,wa);if(a0(c||wa),!n.get(RA,!0))return xa(t)?n.get(nn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(xa(t)){let d=n.get(nn);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return kA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var kA;function OA(t,n,e){try{let i=e();return Qi(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Ad=null;function FA(t=[],n){return ce.create({name:n,providers:[{provide:ea,useValue:"platform"},{provide:mm,useValue:new Set([()=>Ad=null])},...t]})}function PA(t=[]){if(Ad)return Ad;let n=FA(t);return Ad=n,o0(),LA(n),n}function LA(t){let n=t.get(Hl,null);ut(t,()=>{n?.forEach(e=>e())})}function k0(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;be(he.BootstrapApplicationStart);try{let o=r?.injector??PA(i),s=[_0(),Ly,...e||[]],a=new pa({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return NA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{be(he.BootstrapApplicationEnd)}}function q(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function _n(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var dm=Symbol("NOT_SET"),O0=new Set,VA=B(C({},Ps),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:dm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==dm&&!_o(this))return this.signal;try{for(let r of this.cleanup??O0)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=ci(this),i;try{i=this.userFn.apply(null,n)}finally{Fi(this,e)}return(this.value===dm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),gm=class extends fa{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(_t),s),this.scheduler=r;for(let a of Ap){let c=e[a];if(c===void 0)continue;let l=Object.create(VA);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Oi(l),l.value),l.signal[Ue]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??O0)e()}finally{Pi(n)}}};function F0(t,n){let e=n?.injector??u(ce),i=e.get(zn),r=e.get(yd),o=e.get(Kn,null,{optional:!0});r.impl??=e.get(Rp);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Bo,null,{optional:!0}),c=new gm(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function kd(t,n){let e=Vi(t),i=n.elementInjector||No();return new Wo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var P0=null;function bn(){return P0}function ym(t){P0??=t}var Sa=class{},Xo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>u(L0),providedIn:"platform"})}return t})();var L0=(()=>{class t extends Xo{_location;_history;_doc=u(V);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return bn().getBaseHref(this._doc)}onPopState(e){let i=bn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=bn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function B0(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function V0(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function er(t){return t&&t[0]!=="?"?`?${t}`:t}var Jo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:()=>u(BA),providedIn:"root"})}return t})(),jA=new _(""),BA=(()=>{class t extends Jo{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(V).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return B0(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+er(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+er(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+er(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(M(Xo),M(jA,8))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var tr=(()=>{class t{_subject=new S;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=zA(V0(j0(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+er(i))}normalize(e){return t.stripTrailingSlash(HA(this._basePath,j0(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+er(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+er(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=er;static joinWithSlash=B0;static stripTrailingSlash=V0;static \u0275fac=function(i){return new(i||t)(M(Jo))};static \u0275prov=F({token:t,factory:()=>UA(),providedIn:"root"})}return t})();function UA(){return new tr(M(Jo))}function HA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function j0(t){return t.replace(/\/index\.html$/,"")}function zA(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Od=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Cn=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Od(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),U0(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);U0(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(w(qt),w(gn),w(Nd))};static \u0275dir=P({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function U0(t,n){t.context.$implicit=n.item}var ct=(()=>{class t{_viewContainer;_context=new Fd;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){H0(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){H0(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(w(qt),w(gn))};static \u0275dir=P({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Fd=class{$implicit=null;ngIf=null};function H0(t,n){if(t&&!t.createEmbeddedView)throw new E(2020,!1)}var Ma=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(ce);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(w(qt))};static \u0275dir=P({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ke]})}return t})();var tt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})();function Ta(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var _m="browser";function z0(t){return t===_m}var Aa=class{_doc;constructor(n){this._doc=n}manager},Pd=(()=>{class t extends Aa{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),jd=new _(""),wm=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Pd));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Pd);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new E(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(M(jd),M(O))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),bm="ng-app-id";function $0(t){for(let n of t)n.remove()}function G0(t,n){let e=n.createElement("style");return e.textContent=t,e}function GA(t,n,e,i){let r=t.head?.querySelectorAll(`style[${bm}="${n}"],link[${bm}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(bm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Em(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Dm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,GA(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,G0);i?.forEach(r=>this.addUsage(r,this.external,Em))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&($0(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])$0(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,G0(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Em(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(M(V),M(Ur),M(zr,8),M(Hr))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),Cm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},xm=/%COMP%/g;var q0="%COMP%",WA=`_nghost-${q0}`,qA=`_ngcontent-${q0}`,YA=!0,ZA=new _("",{factory:()=>YA});function KA(t){return qA.replace(xm,t)}function QA(t){return WA.replace(xm,t)}function Y0(t,n){return n.map(e=>e.replace(xm,t))}var Im=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Ra(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Vd?r.applyToHost(e):r instanceof Na&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Fn.Emulated:o=new Vd(c,l,i,this.appId,d,s,a,f);break;case Fn.ShadowDom:return new Ld(c,e,i,s,a,this.nonce,f,l);case Fn.ExperimentalIsolatedShadowDom:return new Ld(c,e,i,s,a,this.nonce,f);default:o=new Na(c,l,i,d,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(M(wm),M(Yr),M(Ur),M(ZA),M(V),M(O),M(zr),M(Kn,8))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})(),Ra=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Cm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(W0(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(W0(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new E(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Cm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Cm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Yn.DashCase|Yn.Important)?n.style.setProperty(e,i,r&Yn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Yn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=bn().getGlobalEventTarget(this.doc,n),!n))throw new E(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function W0(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Ld=class extends Ra{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Y0(i.id,l);for(let f of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=f,this.shadowRoot.appendChild(m)}let d=i.getExternalStyles?.();if(d)for(let f of d){let m=Em(f,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Na=class extends Ra{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Y0(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Gr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Vd=class extends Na{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=KA(l),this.hostAttr=QA(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Bd=class t extends Sa{supportsDOMEvents=!0;static makeCurrent(){ym(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=XA();return e==null?null:JA(e)}resetBaseElement(){ka=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ta(document.cookie,n)}},ka=null;function XA(){return ka=ka||document.head.querySelector("base"),ka?ka.getAttribute("href"):null}function JA(t){return new URL(t,document.baseURI).pathname}var Z0=["alt","control","meta","shift"],eR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},tR={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},K0=(()=>{class t extends Aa{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>bn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),Z0.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=eR[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Z0.forEach(s=>{if(s!==r){let a=tR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac})}return t})();function Sm(t,n,e){return Ce(this,null,function*(){let i=C({rootComponent:t},nR(n,e));return k0(i)})}function nR(t,n){return{platformRef:n?.platformRef,appProviders:[...aR,...t?.providers??[]],platformProviders:sR}}function iR(){Bd.makeCurrent()}function rR(){return new Vt}function oR(){return _p(document),document}var sR=[{provide:Hr,useValue:_m},{provide:Hl,useValue:iR,multi:!0},{provide:V,useFactory:oR}];var aR=[{provide:ea,useValue:"root"},{provide:Vt,useFactory:rR},{provide:jd,useClass:Pd,multi:!0},{provide:jd,useClass:K0,multi:!0},Im,{provide:Yr,useClass:Dm},{provide:Dm,useExisting:Yr},wm,{provide:Ze,useExisting:Im},[]];var Mi=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var Hd=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},zd=class{encodeKey(n){return Q0(n)}encodeValue(n){return Q0(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function cR(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var lR=/%(\d[a-f0-9])/gi,dR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Q0(t){return encodeURIComponent(t).replace(lR,(n,e)=>dR[e]??n)}function Ud(t){return`${t}`}var Si=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new zd,n.fromString){if(n.fromObject)throw new E(2805,!1);this.map=cR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Ud):[Ud(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Ud(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Ud(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function uR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function X0(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function J0(t){return typeof Blob<"u"&&t instanceof Blob}function eC(t){return typeof FormData<"u"&&t instanceof FormData}function fR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Mm="Content-Type",tC="Accept",iC="text/plain",rC="application/json",hR=`${rC}, ${iC}, */*`,es=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(uR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new E(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Mi,this.context??=new Hd,!this.params)this.params=new Si,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||X0(this.body)||J0(this.body)||eC(this.body)||fR(this.body)?this.body:this.body instanceof Si?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||eC(this.body)?null:J0(this.body)?this.body.type||null:X0(this.body)?null:typeof this.body=="string"?iC:this.body instanceof Si?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?rC:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,m=n.integrity||this.integrity,g=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,x=n.timeout??this.timeout,R=n.body!==void 0?n.body:this.body,ee=n.withCredentials??this.withCredentials,it=n.reportProgress??this.reportProgress,Et=n.reportUploadProgress??this.reportUploadProgress,go=n.reportDownloadProgress??this.reportDownloadProgress,Rs=n.headers||this.headers,hr=n.params||this.params,Rc=n.context??this.context;return n.setHeaders!==void 0&&(Rs=Object.keys(n.setHeaders).reduce((vo,pr)=>vo.set(pr,n.setHeaders[pr]),Rs)),n.setParams&&(hr=Object.keys(n.setParams).reduce((vo,pr)=>vo.set(pr,n.setParams[pr]),hr)),new t(e,i,R,{params:hr,headers:Rs,context:Rc,reportProgress:it,reportUploadProgress:Et,reportDownloadProgress:go,responseType:r,withCredentials:ee,transferCache:b,keepalive:o,cache:a,priority:s,timeout:x,mode:c,redirect:l,credentials:d,referrer:f,integrity:m,referrerPolicy:g})}},ts=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(ts||{}),ns=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Mi,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},$d=class t extends ns{constructor(n={}){super(n)}type=ts.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Oa=class t extends ns{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=ts.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Qr=class extends ns{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},pR=200;var mR=/^\)\]\}',?\n/,aq=1024*1024,oC=new _("",{factory:()=>null}),Gd=(()=>{class t{fetchImpl=u(Am,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(O);destroyRef=u(_t);maxResponseSize=u(oC);handle(e){return new te(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(Rm,s=>i.error(new Qr({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return Ce(this,null,function*(){let o=this.createRequestInit(e),s;try{let R=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));gR(R),r.next({type:ts.Sent}),s=yield R}catch(R){r.error(new Qr({error:R,status:R.status??0,statusText:R.statusText,url:e.urlWithParams,headers:R.headers}));return}let a=new Mi(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,f=null,m=e.reportProgress||e.reportDownloadProgress;if(m&&r.next(new $d({headers:a,status:d,statusText:c,url:l})),s.body){let R=s.headers.get("content-length"),ee=R!==null?Number(R):NaN;this.maxResponseSize!==null&&Number.isFinite(ee)&&ee>this.maxResponseSize&&nC(this.maxResponseSize);let it=[],Et=s.body.getReader(),go=0,Rs,hr,Rc=typeof Zone<"u"&&Zone.current,vo=!1;if(yield this.ngZone.runOutsideAngular(()=>Ce(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield Et.cancel(),vo=!0;break}let{done:Ns,value:cf}=yield Et.read();if(Ns)break;if(it.push(cf),go+=cf.length,this.maxResponseSize!==null&&go>this.maxResponseSize&&(yield Et.cancel(),nC(this.maxResponseSize)),m){hr=e.responseType==="text"?(hr??"")+(Rs??=new TextDecoder).decode(cf,{stream:!0}):void 0;let nv=()=>r.next({type:ts.DownloadProgress,total:Number.isFinite(ee)?ee:void 0,loaded:go,partialText:hr});Rc?Rc.run(nv):nv()}}})),vo){r.complete();return}let pr=this.concatChunks(it,go);try{let Ns=s.headers.get(Mm)??"";f=this.parseBody(e,pr,Ns,d)}catch(Ns){r.error(new Qr({error:Ns,headers:new Mi(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=f?pR:0);let g=d>=200&&d<300,b=s.redirected,x=s.type;g?(r.next(new Oa({body:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:x})),r.complete()):r.error(new Qr({error:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:x}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(mR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new E(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(tC)||(i[tC]=hR),!e.headers.has(Mm)){let o=e.detectContentTypeHeader();o!==null&&(i[Mm]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Am=class{};function Rm(){}function gR(t){t.then(Rm,Rm)}function nC(t){throw new E(-2825,!1)}function vR(t,n){return n(t)}function yR(t,n,e){return(i,r)=>ut(e,()=>n(i,o=>t(o,r)))}var Nm=new _("",{factory:()=>[]}),sC=new _(""),aC=new _("",{factory:()=>!0});var km=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(Gd),r},providedIn:"root"})}return t})();var Wd=(()=>{class t{backend;injector;chain=null;pendingTasks=u(aa);contributeToStability=u(aC);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Nm),...this.injector.get(sC,[])]));this.chain=i.reduceRight((r,o)=>yR(r,o,this.injector),vR)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Sr(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(M(km),M(Ee))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Om=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(Wd),r},providedIn:"root"})}return t})();function Tm(t,n){return C({body:n},t)}var En=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof es)o=e;else{let c;r.headers instanceof Mi?c=r.headers:c=new Mi(r.headers);let l;r.params&&(r.params instanceof Si?l=r.params:l=new Si({fromObject:r.params})),o=new es(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=L(o).pipe(Mo(c=>this.handler.handle(c)));if(e instanceof es||r.observe==="events")return s;let a=s.pipe(Te(c=>c instanceof Oa));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(X(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new E(2806,!1);return c.body}));case"blob":return a.pipe(X(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new E(2807,!1);return c.body}));case"text":return a.pipe(X(c=>{if(c.body!==null&&typeof c.body!="string")throw new E(2808,!1);return c.body}));default:return a.pipe(X(c=>c.body))}case"response":return a;default:throw new E(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Si().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Tm(r,i))}post(e,i,r={}){return this.request("POST",e,Tm(r,i))}put(e,i,r={}){return this.request("PUT",e,Tm(r,i))}static \u0275fac=function(i){return new(i||t)(M(Om))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _R=new _("",{factory:()=>!0}),bR="XSRF-TOKEN",CR=new _("",{factory:()=>bR}),ER="X-XSRF-TOKEN",wR=new _("",{factory:()=>ER}),DR=(()=>{class t{cookieName=u(CR);doc=u(V);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Ta(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),cC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(DR),r},providedIn:"root"})}return t})();function xR(t,n){if(!u(_R)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(Xo).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch(r){return n(t)}let e=u(cC).getToken(),i=u(wR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var Fm=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t[t.Xhr=7]="Xhr",t})(Fm||{});function IR(t,n){return{\u0275kind:t,\u0275providers:n}}function Pm(...t){let n=[En,Gd,Wd,{provide:Om,useExisting:Wd},{provide:km,useFactory:()=>u(Gd)},{provide:Nm,useValue:xR,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return pi(n)}function Lm(t){return IR(Fm.Interceptors,t.map(n=>({provide:Nm,useValue:n,multi:!0})))}var lC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(M(V))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Pa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=F({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(SR),r},providedIn:"root"})}return t})(),SR=(()=>{class t extends Pa{_doc=u(V);sanitize(e,i){if(i==null)return null;switch(e){case st.NONE:return i;case st.HTML:return wi(i,"HTML")?Pn(i):Ip(this._doc,String(i)).toString();case st.STYLE:return wi(i,"Style")?Pn(i):i;case st.SCRIPT:if(wi(i,"Script"))return Pn(i);throw new E(5200,!1);case st.URL:return wi(i,"URL")?Pn(i):ya(String(i));case st.RESOURCE_URL:if(wi(i,"ResourceURL"))return Pn(i);throw new E(5201,!1);default:throw new E(5202,!1)}}bypassSecurityTrustHtml(e){return Cp(e)}bypassSecurityTrustStyle(e){return Ep(e)}bypassSecurityTrustScript(e){return wp(e)}bypassSecurityTrustUrl(e){return Dp(e)}bypassSecurityTrustResourceUrl(e){return xp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Z="primary",Ka=Symbol("RouteTitle"),Hm=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Jr(t){return new Hm(t)}function Vm(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function _C(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return Vm(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Vm(o,t.slice(0,o.length),a)||!Vm(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function Xd(t){return new Promise((n,e)=>{t.pipe(di()).subscribe({next:i=>n(i),error:i=>e(i)})})}function MR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!ti(t[e],n[e]))return!1;return!0}function ti(t,n){let e=t?zm(t):void 0,i=n?zm(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!bC(t[r],n[r]))return!1;return!0}function zm(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function bC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function TR(t){return t.length>0?t[t.length-1]:null}function no(t){return Hs(t)?t:Qi(t)?Ne(Promise.resolve(t)):L(t)}function CC(t){return Hs(t)?Xd(t):Promise.resolve(t)}var AR={exact:DC,subset:xC},EC={exact:RR,subset:NR,ignored:()=>!0},wC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},$m={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function uC(t,n,e){return AR[e.paths](t.root,n.root,e.matrixParams)&&EC[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function RR(t,n){return ti(t,n)}function DC(t,n,e){if(!Xr(t.segments,n.segments)||!Zd(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!DC(t.children[i],n.children[i],e))return!1;return!0}function NR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>bC(t[e],n[e]))}function xC(t,n,e){return IC(t,n,n.segments,e)}function IC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Xr(r,e)||n.hasChildren()||!Zd(r,e,i))}else if(t.segments.length===e.length){if(!Xr(t.segments,e)||!Zd(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!xC(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Xr(t.segments,r)||!Zd(t.segments,r,i)||!t.children[Z]?!1:IC(t.children[Z],n,o,i)}}function Zd(t,n,e){return n.every((i,r)=>EC[e](t[r].parameters,i.parameters))}var on=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ye([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Jr(this.queryParams),this._queryParamMap}toString(){return FR.serialize(this)}},ye=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Kd(this)}},nr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Jr(this.parameters),this._parameterMap}toString(){return MC(this)}};function kR(t,n){return Xr(t,n)&&t.every((e,i)=>ti(e.parameters,n[i].parameters))}function Xr(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function OR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===Z&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==Z&&(e=e.concat(n(r,i)))}),e}var us=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>new ir})}return t})(),ir=class{parse(n){let e=new Wm(n);return new on(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${La(n.root,!0)}`,i=VR(n.queryParams),r=typeof n.fragment=="string"?`#${PR(n.fragment)}`:"";return`${e}${i}${r}`}},FR=new ir;function Kd(t){return t.segments.map(n=>MC(n)).join("/")}function La(t,n){if(!t.hasChildren())return Kd(t);if(n){let e=t.children[Z]?La(t.children[Z],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==Z&&i.push(`${r}:${La(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=OR(t,(i,r)=>r===Z?[La(t.children[Z],!1)]:[`${r}:${La(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[Z]!=null?`${Kd(t)}/${e[0]}`:`${Kd(t)}/(${e.join("//")})`}}function SC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function qd(t){return SC(t).replace(/%3B/gi,";")}function PR(t){return encodeURI(t)}function Gm(t){return SC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Qd(t){return decodeURIComponent(t)}function fC(t){return Qd(t.replace(/\+/g,"%20"))}function MC(t){return`${Gm(t.path)}${LR(t.parameters)}`}function LR(t){return Object.entries(t).map(([n,e])=>`;${Gm(n)}=${Gm(e)}`).join("")}function VR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${qd(e)}=${qd(r)}`).join("&"):`${qd(e)}=${qd(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var jR=/^[^\/()?;#]+/;function jm(t){let n=t.match(jR);return n?n[0]:""}var BR=/^[^\/()?;=#]+/;function UR(t){let n=t.match(BR);return n?n[0]:""}var HR=/^[^=?&#]+/;function zR(t){let n=t.match(HR);return n?n[0]:""}var $R=/^[^&#]+/;function GR(t){let n=t.match($R);return n?n[0]:""}var Wm=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ye([],{}):new ye([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new E(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[Z]=new ye(e,i)),r}parseSegment(){let n=jm(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new E(4009,!1);return this.capture(n),new nr(Qd(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=UR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=jm(this.remaining);r&&(i=r,this.capture(i))}n[Qd(e)]=Qd(i)}parseQueryParam(n){let e=zR(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=GR(this.remaining);s&&(i=s,this.capture(i))}let r=fC(e),o=fC(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=jm(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new E(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=Z);let a=this.parseChildren(e+1);i[s??Z]=Object.keys(a).length===1&&a[Z]?a[Z]:new ye([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new E(4011,!1)}};function TC(t){return t.segments.length>0?new ye([],{[Z]:t}):t}function AC(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=AC(r);if(i===Z&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new ye(t.segments,n);return WR(e)}function WR(t){if(t.numberOfChildren===1&&t.children[Z]){let n=t.children[Z];return new ye(t.segments.concat(n.segments),n.children)}return t}function rr(t){return t instanceof on}function RC(t,n,e=null,i=null,r=new ir){let o=NC(t);return kC(o,n,e,i,r)}function NC(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new ye(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=TC(i);return n??r}function kC(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Bm(o,o,o,e,i,r);let s=qR(n);if(s.toRoot())return Bm(o,o,new ye([],{}),e,i,r);let a=YR(s,o,t),c=a.processChildren?ja(a.segmentGroup,a.index,s.commands):FC(a.segmentGroup,a.index,s.commands);return Bm(o,a.segmentGroup,c,e,i,r)}function Jd(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Ha(t){return typeof t=="object"&&t!=null&&t.outlets}function hC(t,n,e){t||="\u0275";let i=new on;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Bm(t,n,e,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>hC(l,f,o)):hC(l,d,o);let a;t===n?a=e:a=OC(t,n,e);let c=TC(AC(a));return new on(c,s,r)}function OC(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=OC(o,n,e)}),new ye(t.segments,i)}var eu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Jd(i[0]))throw new E(4003,!1);let r=i.find(Ha);if(r&&r!==TR(i))throw new E(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function qR(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new eu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new eu(e,n,i)}var rs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function YR(t,n,e){if(t.isAbsolute)return new rs(n,!0,0);if(!e)return new rs(n,!1,NaN);if(e.parent===null)return new rs(e,!0,0);let i=Jd(t.commands[0])?0:1,r=e.segments.length-1+i;return ZR(e,r,t.numberOfDoubleDots)}function ZR(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new E(4005,!1);r=i.segments.length}return new rs(i,!1,r-o)}function KR(t){return Ha(t[0])?t[0].outlets:{[Z]:t}}function FC(t,n,e){if(t??=new ye([],{}),t.segments.length===0&&t.hasChildren())return ja(t,n,e);let i=QR(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new ye(t.segments.slice(0,i.pathIndex),{});return o.children[Z]=new ye(t.segments.slice(i.pathIndex),t.children),ja(o,0,r)}else return i.match&&r.length===0?new ye(t.segments,{}):i.match&&!t.hasChildren()?qm(t,n,e):i.match?ja(t,0,r):qm(t,n,e)}function ja(t,n,e){if(e.length===0)return new ye(t.segments,{});{let i=KR(e),r={};if(Object.keys(i).some(o=>o!==Z)&&t.children[Z]&&t.numberOfChildren===1&&t.children[Z].segments.length===0){let o=ja(t.children[Z],n,e);return new ye(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=FC(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new ye(t.segments,r)}}function QR(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(Ha(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!mC(c,l,s))return o;i+=2}else{if(!mC(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function qm(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Ha(o)){let c=XR(o.outlets);return new ye(i,c)}if(r===0&&Jd(e[0])){let c=t.segments[n];i.push(new nr(c.path,pC(e[0]))),r++;continue}let s=Ha(o)?o.outlets[Z]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&Jd(a)?(i.push(new nr(s,pC(a))),r+=2):(i.push(new nr(s,{})),r++)}return new ye(i,{})}function XR(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=qm(new ye([],{}),0,i))}),n}function pC(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function mC(t,n,e){return t==e.path&&ti(n,e.parameters)}var Ba="imperative",ft=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(ft||{}),sn=class{id;url;constructor(n,e){this.id=n,this.url=e}},eo=class extends sn{type=ft.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ni=class extends sn{urlAfterRedirects;type=ft.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},xt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(xt||{}),za=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(za||{}),wn=class extends sn{reason;code;type=ft.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function PC(t){return t instanceof wn&&(t.code===xt.Redirect||t.code===xt.SupersededByNewNavigation)}var Ai=class extends sn{reason;code;type=ft.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},to=class extends sn{error;target;type=ft.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},$a=class extends sn{urlAfterRedirects;state;type=ft.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},tu=class extends sn{urlAfterRedirects;state;type=ft.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},nu=class extends sn{urlAfterRedirects;state;shouldActivate;type=ft.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},iu=class extends sn{urlAfterRedirects;state;type=ft.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ru=class extends sn{urlAfterRedirects;state;type=ft.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ou=class{route;type=ft.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},su=class{route;type=ft.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},au=class{snapshot;type=ft.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},cu=class{snapshot;type=ft.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},lu=class{snapshot;type=ft.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},du=class{snapshot;type=ft.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var ss=class{},Ga=class{},as=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function JR(t){return!(t instanceof ss)&&!(t instanceof as)&&!(t instanceof Ga)}var uu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new fs(this.rootInjector)}},fs=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new uu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(M(Ee))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Ym(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=Ym(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Zm(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Zm(n,this._root).map(e=>e.value)}};function Ym(t,n){if(t===n.value)return n;for(let e of n.children){let i=Ym(t,e);if(i)return i}return null}function Zm(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Zm(t,e);if(i.length)return i.unshift(n),i}return[]}var rn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function is(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var Wa=class extends fu{snapshot;constructor(n,e){super(n),this.snapshot=e,rg(this,n)}toString(){return this.snapshot.toString()}};function LC(t,n){let e=eN(t,n),i=new rt([new nr("",{})]),r=new rt({}),o=new rt({}),s=new rt({}),a=new rt(""),c=new Ht(i,r,s,a,o,Z,t,e.root);return c.snapshot=e.root,new Wa(new rn(c,[]),e)}function eN(t,n){let e={},i={},r={},s=new cs([],e,r,"",i,Z,t,null,{},n);return new qa("",new rn(s,[]))}var Ht=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(X(l=>l[Ka]))??L(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(X(n=>Jr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(X(n=>Jr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},tN="always";function ig(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&jC(r)&&(i.resolve[Ka]=r.title),i}var cs=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Ka]}constructor(n,e,i,r,o,s,a,c,l,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Jr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Jr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},qa=class extends fu{url;constructor(n,e){super(e),this.url=n,rg(this,e)}toString(){return VC(this._root)}};function rg(t,n){n.value._routerState=t,n.children.forEach(e=>rg(t,e))}function VC(t){let n=t.children.length>0?` { ${t.children.map(VC).join(", ")} } `:"";return`${t.value}${n}`}function Um(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,ti(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),ti(n.params,e.params)||t.paramsSubject.next(e.params),MR(n.url,e.url)||t.urlSubject.next(e.url),ti(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Km(t,n){let e=ti(t.params,n.params)&&kR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Km(t.parent,n.parent))}function jC(t){return typeof t.title=="string"||t.title===null}var BC=new _(""),Qa=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Z;activateEvents=new J;deactivateEvents=new J;attachEvents=new J;detachEvents=new J;routerOutletData=Qo();parentContexts=u(fs);location=u(qt);changeDetector=u(me);inputBinder=u(gu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new E(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new E(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new E(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new E(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Qm(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ke]})}return t})(),Qm=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Ht?this.route:n===fs?this.childContexts:n===BC?this.outletData:this.parent.get(n,e)}},gu=new _("");var og=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&le(0,"router-outlet")},dependencies:[Qa],encapsulation:2,changeDetection:1})}return t})();function sg(t){let n=t.children&&t.children.map(sg),e=n?B(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==Z&&(e.component=og),e}function nN(t,n,e){let i=new Set,r=Ya(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new Wa(r,n)}}function Ya(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=iN(t,n,e,i);return new rn(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>Ya(t,c,void 0,i)),a}}let r=rN(n.value);i.add(r);let o=n.children.map(s=>Ya(t,s,void 0,i));return new rn(r,o)}}function iN(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return Ya(t,r,o,i);return Ya(t,r,void 0,i)})}function rN(t){return new Ht(new rt(t.url),new rt(t.params),new rt(t.queryParams),new rt(t.fragment),new rt(t.data),t.outlet,t.component,t)}var ls=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},UC="ngNavigationCancelingError";function hu(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=rr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=HC(!1,xt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function HC(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[UC]=!0,e.cancellationCode=n,e}function oN(t){return zC(t)&&rr(t.url)}function zC(t){return!!t&&t[UC]}var Xm=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Um(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=is(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=is(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=is(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=is(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new du(o.value.snapshot))}),n.children.length&&this.forwardEvent(new cu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Um(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Um(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},pu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},os=class{component;route;constructor(n,e){this.component=n,this.route=e}};function sN(t,n,e){let i=t._root,r=n?n._root:null;return Va(i,r,e,[i.value])}function aN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function hs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Gf(t)?t:n.get(t):i}function Va(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=is(n);return t.children.forEach(s=>{cN(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Ua(a,e.getContext(s),r)),r}function cN(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=lN(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new pu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Va(t,n,a?a.children:null,i,r):Va(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new os(a.outlet.component,s))}else s&&Ua(n,a,r),r.canActivateChecks.push(new pu(i)),o.component?Va(t,null,a?a.children:null,i,r):Va(t,null,e,i,r);return r}function lN(t,n,e){if(typeof e=="function")return ut(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Xr(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Xr(t.url,n.url)||!ti(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Km(t,n)||!ti(t.queryParams,n.queryParams);default:return!Km(t,n)}}function Ua(t,n,e){let i=is(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Ua(s,n.children.getContext(o),e):Ua(s,null,e):Ua(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new os(n.outlet.component,r)):e.canDeactivateChecks.push(new os(null,r)):e.canDeactivateChecks.push(new os(null,r))}function Xa(t){return typeof t=="function"}function dN(t){return typeof t=="boolean"}function uN(t){return t&&Xa(t.canLoad)}function fN(t){return t&&Xa(t.canActivate)}function hN(t){return t&&Xa(t.canActivateChild)}function pN(t){return t&&Xa(t.canDeactivate)}function mN(t){return t&&Xa(t.canMatch)}function $C(t){return t instanceof Dr||t?.name==="EmptyError"}var Yd=Symbol("INITIAL_VALUE");function ds(){return ot(t=>Sf(t.map(n=>n.pipe(Qt(1),Xt(Yd)))).pipe(X(n=>{for(let e of n)if(e!==!0){if(e===Yd)return Yd;if(e===!1||gN(e))return e}return!0}),Te(n=>n!==Yd),Qt(1)))}function gN(t){return rr(t)||t instanceof ls}function GC(t){return t.aborted?L(void 0).pipe(Qt(1)):new te(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function WC(t){return Ve(GC(t))}function vN(t){return Lt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?L(B(C({},n),{guardsResult:!0})):yN(o,e,i).pipe(Lt(s=>s&&dN(s)?_N(e,r,t):L(s)),X(s=>B(C({},n),{guardsResult:s})))})}function yN(t,n,e){return Ne(t).pipe(Lt(i=>DN(i.component,i.route,e,n)),di(i=>i!==!0,!0))}function _N(t,n,e){return Ne(n).pipe(Mo(i=>So(CN(i.route.parent,e),bN(i.route,e),wN(t,i.path),EN(t,i.route))),di(i=>i!==!0,!0))}function bN(t,n){return t!==null&&n&&n(new lu(t)),L(!0)}function CN(t,n){return t!==null&&n&&n(new au(t)),L(!0)}function EN(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return L(!0);let i=e.map(r=>xr(()=>{let o=n._environmentInjector,s=hs(r,o),a=fN(s)?s.canActivate(n,t):ut(o,()=>s(n,t));return no(a).pipe(di())}));return L(i).pipe(ds())}function wN(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>aN(o)).filter(o=>o!==null).map(o=>xr(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=hs(a,c),d=hN(l)?l.canActivateChild(e,t):ut(c,()=>l(e,t));return no(d).pipe(di())});return L(s).pipe(ds())}));return L(r).pipe(ds())}function DN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return L(!0);let o=r.map(s=>{let a=n._environmentInjector,c=hs(s,a),l=pN(c)?c.canDeactivate(t,n,e,i):ut(a,()=>c(t,n,e,i));return no(l).pipe(di())});return L(o).pipe(ds())}function xN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return L(!0);let s=o.map(a=>{let c=hs(a,t),l=uN(c)?c.canLoad(n,e):ut(t,()=>c(n,e)),d=no(l);return r?d.pipe(WC(r)):d});return L(s).pipe(ds(),qC(i))}function qC(t){return wf(ze(n=>{if(typeof n!="boolean")throw hu(t,n)}),X(n=>n===!0))}function IN(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return L(!0);let a=s.map(c=>{let l=hs(c,t),d=mN(l)?l.canMatch(n,e,r):ut(t,()=>l(n,e,r));return no(d).pipe(WC(o))});return L(a).pipe(ds(),qC(i))}var Ti=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Za=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function SN(t){throw new E(4e3,!1)}function MN(t){throw HC(!1,xt.GuardRejected)}var Jm=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}lineralizeSegments(n,e){return Ce(this,null,function*(){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Z])throw SN(`${n.redirectTo}`);r=r.children[Z]}})}applyRedirectCommands(n,e,i,r,o){return Ce(this,null,function*(){let s=yield TN(e,r,o);if(s instanceof on)throw new Za(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Za(a);return a})}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new on(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new ye(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new E(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function TN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return Xd(no(ut(e,()=>i(n))))}function AN(t,n){return t.providers&&!t._injector&&(t._injector=Ca(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Vn(t){return t.outlet||Z}function RN(t,n){let e=t.filter(i=>Vn(i)===n);return e.push(...t.filter(i=>Vn(i)!==n)),e}var eg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function YC(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function NN(t,n,e,i,r,o,s){let a=ZC(t,n,e);if(!a.matched)return L(a);let c=YC(o(a));return i=AN(n,i),IN(i,n,e,r,c,s).pipe(X(l=>l===!0?a:C({},eg)))}function ZC(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},eg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||_C)(e,t,n);if(!r)return C({},eg);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function gC(t,n,e,i,r){return e.length>0&&FN(t,e,i,r)?{segmentGroup:new ye(n,ON(i,new ye(e,t.children))),slicedSegments:[]}:e.length===0&&PN(t,e,i)?{segmentGroup:new ye(t.segments,kN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new ye(t.segments,t.children),slicedSegments:e}}function kN(t,n,e,i){let r={};for(let o of e)if(vu(t,n,o)&&!i[Vn(o)]){let s=new ye([],{});r[Vn(o)]=s}return C(C({},i),r)}function ON(t,n){let e={};e[Z]=n;for(let i of t)if(i.path===""&&Vn(i)!==Z){let r=new ye([],{});e[Vn(i)]=r}return e}function FN(t,n,e,i){return e.some(r=>!vu(t,n,r)||!(Vn(r)!==Z)?!1:!(i!==void 0&&Vn(r)===i))}function PN(t,n,e){return e.some(i=>vu(t,n,i))}function vu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function LN(t,n,e){return n.length===0&&!t.children[e]}var tg=class{};function VN(t,n,e,i,r,o,s,a){return Ce(this,null,function*(){return new ng(t,n,e,i,r,s,o,a).recognize()})}var jN=31,ng=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Jm(this.urlSerializer,this.urlTree)}noMatchError(n){return new E(4002,`'${n.segmentGroup}'`)}recognize(){return Ce(this,null,function*(){let n=gC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=yield this.match(n),r=new rn(i,e),o=new qa("",r),s=RC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}})}match(n){return Ce(this,null,function*(){let e=new cs([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Z,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,n,Z,e),rootSnapshot:e}}catch(i){if(i instanceof Za)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ti?this.noMatchError(i):i}})}processSegmentGroup(n,e,i,r,o){return Ce(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=yield this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof rn?[s]:[]})}processChildren(n,e,i,r){return Ce(this,null,function*(){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=RN(e,c),f=yield this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=KC(s);return BN(a),a})}processSegment(n,e,i,r,o,s,a){return Ce(this,null,function*(){for(let c of e)try{return yield this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof Ti||$C(l))continue;throw l}if(LN(i,r,o))return new tg;throw new Ti(i)})}processSegmentAgainstRoute(n,e,i,r,o,s,a,c){return Ce(this,null,function*(){if(Vn(i)!==s&&(s===Z||!vu(r,o,i)))throw new Ti(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new Ti(r)})}expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){return Ce(this,null,function*(){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:m}=ZC(e,r,o);if(!c)throw new Ti(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>jN&&(this.allowRedirects=!1));let g=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=yield this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,YC(g),n),x=yield this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,x.concat(m),s,!1,a)})}createSnapshot(n,e,i,r,o){let s=new cs(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,HN(e),Vn(e),e.component??e._loadedComponent??null,e,zN(e),n),a=ig(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}matchSegmentAgainstRoute(n,e,i,r,o,s){return Ce(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Et=>this.createSnapshot(n,i,Et.consumedSegments,Et.parameters,s),c=yield Xd(NN(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new Ti(e);n=i._injector??n;let{routes:l}=yield this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:m,remainingSegments:g}=c,b=this.createSnapshot(n,i,m,f,s),{segmentGroup:x,slicedSegments:R}=gC(e,m,g,l,o);if(R.length===0&&x.hasChildren()){let Et=yield this.processChildren(d,l,x,b);return new rn(b,Et)}if(l.length===0&&R.length===0)return new rn(b,[]);let ee=Vn(i)===o,it=yield this.processSegment(d,l,x,R,ee?Z:o,!0,b);return new rn(b,it instanceof rn?[it]:[])})}getChildConfig(n,e,i){return Ce(this,null,function*(){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield Xd(xN(n,e,i,this.urlSerializer,this.abortSignal))){let o=yield this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw MN(e)}return{routes:[],injector:n}})}};function BN(t){t.sort((n,e)=>n.value.outlet===Z?-1:e.value.outlet===Z?1:n.value.outlet.localeCompare(e.value.outlet))}function UN(t){let n=t.value.routeConfig;return n&&n.path===""}function KC(t){let n=[],e=new Set;for(let i of t){if(!UN(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=KC(i.children);n.push(new rn(i.value,r))}return n.filter(i=>!e.has(i))}function HN(t){return t.data||{}}function zN(t){return t.resolve||{}}function $N(t,n,e,i,r,o,s){return Lt(a=>Ce(null,null,function*(){let{state:c,tree:l}=yield VN(t,n,e,i,a.extractedUrl,r,o,s);return B(C({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function GN(t){return Lt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return L(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of QC(a))o.add(c);let s=0;return Ne(o).pipe(Mo(a=>r.has(a)?WN(a,e,t):(a.data=ig(a,a.parent,t).resolve,L(void 0))),ze(()=>s++),ul(1),Lt(a=>s===o.size?L(n):He))})}function QC(t){let n=t.children.map(e=>QC(e)).flat();return[t,...n]}function WN(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!jC(i)&&(r[Ka]=i.title),xr(()=>(t.data=ig(t,t.parent,e).resolve,qN(r,t,n).pipe(X(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function qN(t,n,e){let i=zm(t);if(i.length===0)return L({});let r={};return Ne(i).pipe(Lt(o=>YN(t[o],n,e).pipe(di(),ze(s=>{if(s instanceof ls)throw hu(new ir,s);r[o]=s}))),ul(1),X(()=>r),Ir(o=>$C(o)?He:Us(o)))}function YN(t,n,e){let i=n._environmentInjector,r=hs(t,i),o=r.resolve?r.resolve(n,e):ut(i,()=>r(n,e));return no(o)}function vC(t){return ot(n=>{let e=t(n);return e?Ne(e).pipe(X(()=>n)):L(n)})}var ag=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===Z);return i}getResolvedTitleForRoute(e){return e.data[Ka]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(XC)})}return t})(),XC=(()=>{class t extends ag{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(M(lC))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ps=new _("",{factory:()=>({})}),Ja=new _(""),JC=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(cm);loadComponent(e,i){return Ce(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=Ce(this,null,function*(){try{let o=yield CC(ut(e,()=>i.loadComponent())),s=yield tE(vm(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=Ce(this,null,function*(){try{let o=yield eE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function eE(t,n,e,i){return Ce(this,null,function*(){let r=yield CC(ut(e,()=>t.loadChildren())),o=yield tE(vm(r)),s;o instanceof Dd||Array.isArray(o)?s=o:s=yield n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,d=s,c=a.get(Ja,[],{optional:!0,self:!0}).flat()),{routes:c.map(sg),injector:a,factory:d}})}function tE(t){return Ce(this,null,function*(){return t})}var yu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(ZN)})}return t})(),ZN=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),nE=new _("");var iE=new _(""),KN=()=>{},rE=new _(""),oE=(()=>{class t{currentNavigation=re(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=re(null);events=new S;transitionAbortWithErrorSubject=new S;configLoader=u(JC);environmentInjector=u(Ee);destroyRef=u(_t);urlSerializer=u(us);rootContexts=u(fs);location=u(tr);inputBindingEnabled=u(gu,{optional:!0})!==null;titleStrategy=u(ag);options=u(ps,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||tN;urlHandlingStrategy=u(yu);createViewTransition=u(nE,{optional:!0});navigationErrorHandler=u(rE,{optional:!0});activatedRouteInjectorFeature=u(iE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>L(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new ou(r)),i=r=>this.events.next(new su(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;fe(()=>{this.transitions?.next(B(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new rt(null),this.transitions.pipe(Te(i=>i!==null),ot(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return L(i).pipe(ot(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",xt.SupersededByNewNavigation),He;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?B(C({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new Ai(c.id,this.urlSerializer.serialize(c.rawUrl),"",za.IgnoredSameUrlNavigation)),c.resolve(!1),He;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return L(c).pipe(ot(m=>(this.events.next(new eo(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?He:Promise.resolve(m))),$N(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),ze(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=m.urlAfterRedirects,g)),this.events.next(new Ga)}),ot(m=>Ne(i.routesRecognizeHandler.deferredHandle??L(void 0)).pipe(X(()=>m))),ze(()=>{let m=new $a(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:m,extractedUrl:g,source:b,restoredState:x,extras:R}=c,ee=new eo(m,this.urlSerializer.serialize(g),b,x);this.events.next(ee);let it=LC(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=B(C({},c),{targetSnapshot:it,urlAfterRedirects:g,extras:B(C({},R),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Et=>(Et.finalUrl=g,Et)),L(i)}else return this.events.next(new Ai(c.id,this.urlSerializer.serialize(c.extractedUrl),"",za.IgnoredByUrlHandlingStrategy)),c.resolve(!1),He}),X(c=>{let l=new tu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=B(C({},c),{guards:sN(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),vN(c=>this.events.next(c)),ot(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw hu(this.urlSerializer,c.guardsResult);let l=new nu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return He;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",xt.GuardRejected),He;if(c.guards.canActivateChecks.length===0)return L(c);let d=new iu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!a())return He;let f=!1;return L(c).pipe(GN(this.paramsInheritanceStrategy),ze({next:()=>{f=!0;let m=new ru(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)},complete:()=>{f||this.cancelNavigationTransition(c,"",xt.NoDataFromResolver)}}))}),vC(c=>{let l=f=>{let m=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let g=f._environmentInjector;m.push(this.configLoader.loadComponent(g,f.routeConfig).then(b=>{f.component=b}))}for(let g of f.children)m.push(...l(g));return m},d=l(c.targetSnapshot.root);return d.length===0?L(c):Ne(Promise.all(d).then(()=>c))}),ot(c=>{let{newlyCreatedRoutes:l,state:d}=nN(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=B(C({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),L(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),vC(()=>this.afterPreactivation()),ot(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?Ne(d).pipe(X(()=>i)):L(i)}),Qt(1),ot(c=>{r=!1,this.events.next(new ss);let l=i.beforeActivateHandler.deferredHandle;return l?Ne(l.then(()=>c)):L(c)}),ze(c=>{new Xm(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=KN,l)),this.lastSuccessfulNavigation.set(fe(this.currentNavigation)),this.events.next(new ni(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),Ve(GC(s.signal).pipe(Te(()=>!o&&r),ze(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",xt.Aborted)}))),ze({complete:()=>{o=!0}}),Ve(this.transitionAbortWithErrorSubject.pipe(ze(c=>{throw c}))),Sr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",xt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Ir(c=>{if(o=!0,yC(i),this.destroyed)return i.resolve(!1),He;if(zC(c))this.events.next(new wn(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),oN(c)?this.events.next(new as(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new to(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=ut(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof ls){let{message:f,cancellationCode:m}=hu(this.urlSerializer,d);this.events.next(new wn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,m)),this.events.next(new as(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return He}))}))}cancelNavigationTransition(e,i,r){yC(e);let o=new wn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=fe(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function QN(t){return t!==Ba}function yC(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var sE=new _("");var aE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(XN)})}return t})(),mu=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},XN=(()=>{class t extends mu{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),_u=(()=>{class t{urlSerializer=u(us);options=u(ps,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(tr);urlHandlingStrategy=u(yu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new on;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof on?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=LC(null,u(Ee));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(JN)})}return t})(),JN=(()=>{class t extends _u{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof eo?this.updateStateMemento():e instanceof Ai?this.commitTransition(i):e instanceof $a?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ss?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof wn&&!PC(e)?this.restoreHistory(i):e instanceof to?this.restoreHistory(i,!0):e instanceof ni&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=C(C({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=C(C({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function cg(t,n){t.events.pipe(Te(e=>e instanceof ni||e instanceof wn||e instanceof to||e instanceof Ai),X(e=>e instanceof ni||e instanceof Ai?0:(e instanceof wn?e.code===xt.Redirect||e.code===xt.SupersededByNewNavigation:!1)?2:1),Te(e=>e!==2),Qt(1)).subscribe(()=>{n()})}var Le=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(xd);stateManager=u(_u);options=u(ps,{optional:!0})||{};pendingTasks=u(_i);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(oE);urlSerializer=u(us);location=u(tr);urlHandlingStrategy=u(yu);injector=u(Ee);_events=new S;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(aE);injectorCleanup=u(sE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(Ja,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(gu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ue;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=fe(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof wn&&i.code!==xt.Redirect&&i.code!==xt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ni)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof as){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||QN(r.source)},s);this.scheduleNavigation(a,Ba,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}JR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ba,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=B(C({},o),{browserUrl:e})),r){let l=C({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(tn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return fe(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(sg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let m=r?r.snapshot:this.routerState.snapshot.root;f=NC(m)}catch(m){(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return kC(f,e,d,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=rr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ba,null,i)}navigate(e,i={skipLocationChange:!1}){return ek(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch(i){return this.console.warn(hi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},wC):i===!1?r=C({},$m):r=C(C({},$m),i),rr(e))return uC(this.currentUrlTree,e,r);let o=this.parseUrl(e);return uC(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,m)=>{a=f,c=m});let d=this.pendingTasks.add();return cg(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function ek(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new E(4008,!1)}var ik=(()=>{class t{router=u(Le);stateManager=u(_u);fragment=re("");queryParams=re({});path=re("");serializer=u(us);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof ni&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new on(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),nt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new Ji("href"),{optional:!0});reactiveHref=lm(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return fe(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return fe(this._target)}_target=re(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return fe(this._queryParams)}_queryParams=re(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return fe(this._fragment)}_fragment=re(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return fe(this._queryParamsHandling)}_queryParamsHandling=re(void 0);set state(e){this._state.set(e)}get state(){return fe(this._state)}_state=re(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return fe(this._info)}_info=re(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return fe(this._relativeTo)}_relativeTo=re(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return fe(this._preserveFragment)}_preserveFragment=re(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return fe(this._skipLocationChange)}_skipLocationChange=re(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return fe(this._replaceUrl)}_replaceUrl=re(!1);browserUrl=Qo(void 0);isAnchorElement;onChanges=new S;applicationErrorHandler=u(tn);options=u(ps,{optional:!0});reactiveRouterState=u(ik);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=re(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(rr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=C({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Ut(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:rr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return fe(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(w(Le),w(Ht),va("tabindex"),w(Ge),w(z),w(Jo))};static \u0275dir=P({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&j("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&pe("href",r.reactiveHref(),Sp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",q],skipLocationChange:[2,"skipLocationChange","skipLocationChange",q],replaceUrl:[2,"replaceUrl","replaceUrl",q],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[Ke]})}return t})();var rk=new _("");function lg(t,...n){return pi([{provide:Ja,multi:!0,useValue:t},{provide:Ht,useFactory:ok},{provide:Ea,multi:!0,useFactory:sk},n.map(e=>e.\u0275providers)])}function ok(){return u(Le).routerState.root}function sk(){let t=u(ce);return n=>{let e=t.get(nn);if(n!==e.components[0])return;let i=t.get(Le),r=t.get(ak);t.get(ck)===1&&i.initialNavigation(),t.get(lk,null,{optional:!0})?.setUpPreloading(),t.get(rk,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var ak=new _("",{factory:()=>new S}),ck=new _("",{factory:()=>1});var lk=new _("");var gE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(w(Ge),w(z))};static \u0275dir=P({type:t})}return t})(),dk=(()=>{class t extends gE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Zn(t)))(r||t)}})();static \u0275dir=P({type:t,features:[Qe]})}return t})(),vE=new _("");var uk={provide:vE,useExisting:ln(()=>ht),multi:!0};function fk(){let t=bn()?bn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var hk=new _(""),ht=(()=>{class t extends gE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!fk())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(w(Ge),w(z),w(hk,8))};static \u0275dir=P({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&j("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[et([uk]),Qe]})}return t})();function fg(t){return t==null||hg(t)===0}function hg(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Ru=new _(""),pg=new _(""),pk=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,ro=class{static min(n){return mk(n)}static max(n){return gk(n)}static required(n){return yE(n)}static requiredTrue(n){return vk(n)}static email(n){return yk(n)}static minLength(n){return _k(n)}static maxLength(n){return bk(n)}static pattern(n){return Ck(n)}static nullValidator(n){return Cu()}static compose(n){return DE(n)}static composeAsync(n){return xE(n)}};function mk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function gk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function yE(t){return fg(t.value)?{required:!0}:null}function vk(t){return t.value===!0?null:{required:!0}}function yk(t){return fg(t.value)||pk.test(t.value)?null:{email:!0}}function _k(t){return n=>{let e=n.value?.length??hg(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function bk(t){return n=>{let e=n.value?.length??hg(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function Ck(t){if(!t)return Cu;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(fg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Cu(t){return null}function _E(t){return t!=null}function bE(t){return Qi(t)?Ne(t):t}function CE(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function EE(t,n){return n.map(e=>e(t))}function Ek(t){return!t.validate}function wE(t){return t.map(n=>Ek(n)?n:e=>n.validate(e))}function DE(t){if(!t)return null;let n=t.filter(_E);return n.length==0?null:function(e){return CE(EE(e,n))}}function mg(t){return t!=null?DE(wE(t)):null}function xE(t){if(!t)return null;let n=t.filter(_E);return n.length==0?null:function(e){let i=EE(e,n).map(bE);return zs(i).pipe(X(CE))}}function gg(t){return t!=null?xE(wE(t)):null}function cE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function IE(t){return t._rawValidators}function SE(t){return t._rawAsyncValidators}function dg(t){return t?Array.isArray(t)?t:[t]:[]}function Eu(t,n){return Array.isArray(t)?t.includes(n):t===n}function lE(t,n){let e=dg(n);return dg(t).forEach(r=>{Eu(e,r)||e.push(r)}),e}function dE(t,n){return dg(n).filter(e=>!Eu(t,e))}var wu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=mg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=gg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},or=class extends wu{name;get formDirective(){return null}get path(){return null}};var ec="VALID",bu="INVALID",ms="PENDING",tc="DISABLED",sr=class{},Du=class extends sr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},ic=class extends sr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},rc=class extends sr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},gs=class extends sr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},xu=class extends sr{source;constructor(n){super(),this.source=n}},vs=class extends sr{source;constructor(n){super(),this.source=n}};function ME(t){return(Nu(t)?t.validators:t)||null}function wk(t){return Array.isArray(t)?mg(t):t||null}function TE(t,n){return(Nu(n)?n.asyncValidators:t)||null}function Dk(t){return Array.isArray(t)?gg(t):t||null}function Nu(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function xk(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new E(1e3,"");if(!AE(i,e))throw new E(1001,"")}function Ik(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new E(-1002,"")})}var Iu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=re(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return fe(this.statusReactive)}set status(n){fe(()=>this.statusReactive.set(n))}_status=Ut(()=>this.statusReactive());statusReactive=re(void 0);get valid(){return this.status===ec}get invalid(){return this.status===bu}get pending(){return this.status===ms}get disabled(){return this.status===tc}get enabled(){return this.status!==tc}errors;get pristine(){return fe(this.pristineReactive)}set pristine(n){fe(()=>this.pristineReactive.set(n))}_pristine=Ut(()=>this.pristineReactive());pristineReactive=re(!0);get dirty(){return!this.pristine}get touched(){return fe(this.touchedReactive)}set touched(n){fe(()=>this.touchedReactive.set(n))}_touched=Ut(()=>this.touchedReactive());touchedReactive=re(!1);get untouched(){return!this.touched}_events=new S;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(lE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(lE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(dE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(dE(n,this._rawAsyncValidators))}hasValidator(n){return Eu(this._rawValidators,n)}hasAsyncValidator(n){return Eu(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(B(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new rc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new rc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(B(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new ic(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new ic(!0,i))}markAsPending(n={}){this.status=ms;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new gs(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(B(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=tc,this.errors=null,this._forEachChild(r=>{r.disable(B(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Du(this.value,i)),this._events.next(new gs(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(B(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ec,this._forEachChild(i=>{i.enable(B(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(B(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===ec||this.status===ms)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Du(this.value,e)),this._events.next(new gs(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(B(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?tc:ec}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=ms,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=bE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new gs(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new J,this.statusChanges=new J}_calculateStatus(){return this._allControlsDisabled()?tc:this.errors?bu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ms)?ms:this._anyControlsHaveStatus(bu)?bu:ec}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new ic(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new rc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Nu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=wk(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=Dk(this._rawAsyncValidators)}_updateHasRequiredValidator(){fe(()=>this._hasRequired.set(this.hasValidator(ro.required)))}};function AE(t,n){return Object.hasOwn(t,n)}function Sk(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function Mk(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var ug=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var Tk=(()=>{class t{_validator=Cu;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Cu,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,features:[Ke]})}return t})();var Ak={provide:Ru,useExisting:ln(()=>Dn),multi:!0};var Dn=(()=>{class t extends Tk{required;inputName="required";normalizeInput=q;createValidator=e=>yE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Zn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&pe("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[et([Ak]),Qe]})}return t})();var Rk=new _(""),ku=new _("",{factory:()=>vg}),vg="always";function Nk(t,n){return[...n.path,t]}function uE(t,n,e=vg){yg(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),Ok(t,n),Pk(t,n),Fk(t,n),kk(t,n)}function fE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Mu(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Su(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function kk(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function yg(t,n){let e=IE(t);n.validator!==null?t.setValidators(cE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=SE(t);n.asyncValidator!==null?t.setAsyncValidators(cE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Su(n._rawValidators,r),Su(n._rawAsyncValidators,r)}function Mu(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=IE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=SE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Su(n._rawValidators,i),Su(n._rawAsyncValidators,i),e}function Ok(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&RE(t,n)})}function Fk(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&RE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function RE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function Pk(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function NE(t,n){t==null,yg(t,n)}function Lk(t,n){return Mu(t,n)}function Vk(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function jk(t){return Object.getPrototypeOf(t.constructor)===dk}function kE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function Bk(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===ht?e=o:jk(o)?i=o:r=o}),r||i||e||null}function Uk(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var Hk={provide:Rk,useFactory:()=>{let t=u(ii,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},ii=class extends wu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof vs&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=Bk(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(_t)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(me);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new ue,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof vs&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=Sk(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof Dn))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&Mk(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new ug({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=Ut(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Nn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},Tu=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var It=(()=>{class t extends Tu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(w(ii,2))};static \u0275dir=P({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ae("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Qe]})}return t})(),ri=(()=>{class t extends Tu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(w(or,10))};static \u0275dir=P({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ae("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Qe]})}return t})(),Au=class extends Iu{constructor(n,e,i){super(ME(e),TE(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){fe(()=>{Ik(this,!0,n),Object.keys(n).forEach(i=>{xk(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,B(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new vs(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return AE(this.controls,n)?this.controls[n]:null}};var zk={provide:or,useExisting:ln(()=>zt)},nc=Promise.resolve(),zt=(()=>{class t extends or{callSetDisabledState;get submitted(){return fe(this.submittedReactive)}_submitted=Ut(()=>this.submittedReactive());submittedReactive=re(!1);_directives=new Set;form;ngSubmit=new J;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Au({},mg(e),gg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){nc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){nc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){nc.then(()=>{let i=this._findContainer(e.path),r=new Au({});NE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){nc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){nc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),kE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new xu(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(w(Ru,10),w(pg,10),w(ku,8))};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&j("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[et([zk]),Qe]})}return t})();function hE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function pE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var OE=class extends Iu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(ME(e),TE(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Nu(e)&&(e.nonNullable||e.initialValueIsDefault)&&(pE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){fe(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new vs(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){hE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){hE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){pE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var $k=t=>t instanceof OE;var Gk={provide:ii,useExisting:ln(()=>bt)},mE=Promise.resolve(),bt=(()=>{class t extends ii{_changeDetectorRef;callSetDisabledState;control=new OE;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new J;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),Vk(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,uE(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,uE(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){mE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&q(i);mE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?Nk(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(w(or,9),w(Ru,10),w(pg,10),w(vE,10),w(me,8),w(ku,8),w(ce,8),w(Ge,8))};static \u0275dir=P({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[et([Gk,Hk]),Qe,Ke,nm(null)]})}return t})();var oi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var Wk=(()=>{class t extends or{callSetDisabledState;get submitted(){return fe(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Ut(()=>this._submittedReactive());_submittedReactive=re(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Mu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){fE(e.control||null,e,!1),Uk(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,kE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new xu(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(fE(i||null,e),$k(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);NE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&Lk(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){yg(this.form,this),this._oldForm&&Mu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(w(Ru,10),w(pg,10),w(ku,8))};static \u0275dir=P({type:t,features:[Qe,Ke]})}return t})();var qk={provide:or,useExisting:ln(()=>oc)},oc=(()=>{class t extends Wk{form=null;ngSubmit=new J;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Zn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&j("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[et([qk]),Qe]})}return t})();var Yk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})();var St=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:ku,useValue:e.callSetDisabledState??vg}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Yk]})}return t})();var Zk=new _("cdk-dir-doc",{providedIn:"root",factory:()=>u(V)}),Kk=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function FE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?Kk.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var si=(()=>{class t{get value(){return this.valueSignal()}valueSignal=re("ltr");change=new J;constructor(){let e=u(Zk,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(FE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Me=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})();var Qk=["*"];var Xk=new _("MAT_CARD_CONFIG"),Mt=(()=>{class t{appearance;constructor(){let e=u(Xk,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ae("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:Qk,decls:1,vars:0,template:function(i,r){i&1&&(Je(),de(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})();var Tt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me]})}return t})();function sc(t){return t.buttons===0||t.detail===0}function ac(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var _g;function PE(){if(_g==null){let t=typeof document<"u"?document.head:null;_g=!!(t&&(t.createShadowRoot||t.attachShadow))}return _g}function bg(t){if(PE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Ct(t){if(t.composedPath)try{return t.composedPath()[0]}catch(n){}return t.target}var Cg;try{Cg=typeof Intl<"u"&&Intl.v8BreakIterator}catch(t){Cg=!1}var je=(()=>{class t{_platformId=u(Hr);isBrowser=this._platformId?z0(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Cg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var cc;function LE(){if(cc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>cc=!0}))}finally{cc=cc||!1}return cc}function ys(t){return LE()?t:!!t.capture}function an(t){return t instanceof z?t.nativeElement:t}var VE=new _("cdk-input-modality-detector-options"),jE={ignoreKeys:[18,17,224,91,16]},BE=650,Eg={passive:!0,capture:!0},UE=(()=>{class t{_platform=u(je);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new rt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Ct(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<BE||(this._modality.next(sc(e)?"keyboard":"mouse"),this._mostRecentTarget=Ct(e))};_onTouchstart=e=>{if(ac(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Ct(e)};constructor(){let e=u(O),i=u(V),r=u(VE,{optional:!0});if(this._options=C(C({},jE),r),this.modalityDetected=this._modality.pipe(Af(1)),this.modalityChanged=this.modalityDetected.pipe(dl()),this._platform.isBrowser){let o=u(Ze).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Eg),o.listen(i,"mousedown",this._onMousedown,Eg),o.listen(i,"touchstart",this._onTouchstart,Eg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),lc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(lc||{}),HE=new _("cdk-focus-monitor-default-options"),Ou=ys({passive:!0,capture:!0}),dc=(()=>{class t{_ngZone=u(O);_platform=u(je);_inputModalityDetector=u(UE);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(V);_stopInputModalityDetector=new S;constructor(){let e=u(HE,{optional:!0});this._detectionMode=e?.detectionMode||lc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Ct(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=an(e);if(!this._platform.isBrowser||r.nodeType!==1)return L();let o=bg(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new S,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=an(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=an(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===lc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===lc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?BE:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Ct(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Ou),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Ou)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ve(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Ou),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Ou),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Fu=new WeakMap,$t=(()=>{class t{_appRef;_injector=u(ce);_environmentInjector=u(Ee);load(e){let i=this._appRef=this._appRef||this._injector.get(nn),r=Fu.get(i);r||(r={loaders:new Set,refs:[]},Fu.set(i,r),i.onDestroy(()=>{Fu.get(i)?.refs.forEach(o=>o.destroy()),Fu.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(kd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var uc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Pu;function Jk(){if(Pu===void 0&&(Pu=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Pu=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Pu}function oo(t){return Jk()?.createHTML(t)||t}function zE(t,n,e){let i=e.sanitize(st.HTML,n);t.innerHTML=oo(i||"")}function wg(t){return Array.isArray(t)?t:[t]}var $E=new Set,so,Dg=(()=>{class t{_platform=u(je);_nonce=u(zr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):tO}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&eO(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function eO(t,n){if(!$E.has(t))try{so||(so=document.createElement("style"),n&&so.setAttribute("nonce",n),so.setAttribute("type","text/css"),document.head.appendChild(so)),so.sheet&&(so.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),$E.add(t))}catch(e){console.error(e)}}function tO(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var nO=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var GE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({providers:[nO]})}return t})();var WE=new _("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),qE=new _("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),iO=0,xg=(()=>{class t{_ngZone=u(O);_defaultOptions=u(qE,{optional:!0});_liveElement;_document=u(V);_sanitizer=u(Pa);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(WE,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:zE(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${iO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var rO=200,Lu=class{_letterKeyStream=new S;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new S;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:rO;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(ze(e=>this._pressedLetters.push(e)),Mf(n),Te(()=>this._pressedLetters.length>0),X(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function ai(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var _s=class{_items;_activeItemIndex=re(-1);_activeItem=re(null);_wrap=!1;_typeaheadSubscription=ue.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof bi?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):mn(n)&&(this._effectRef=Nn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new S;change=new S;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Lu(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||ai(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return mn(this._items)?this._items():this._items instanceof bi?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var fc=class extends _s{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var hc=class extends _s{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var QE=new Map,At=class t{_appId=u(Ur);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=QE.get(n);return i===void 0?i=0:i++,QE.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})};var ao;function XE(){if(ao==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return ao=!1,ao;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)ao=!0;else{let t=Element.prototype.scrollTo;t?ao=!/\{\s*\[native code\]\s*\}/.test(t.toString()):ao=!1}}return ao}function Mg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var bs,JE=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Tg(){if(bs)return bs;if(typeof document!="object"||!document)return bs=new Set(JE),bs;let t=document.createElement("input");return bs=new Set(JE.filter(n=>(t.setAttribute("type",n),t.type===n))),bs}var oO=new _("MATERIAL_ANIMATIONS"),ew=null;function Ag(){return u(oO,{optional:!0})?.animationsDisabled||u(oa,{optional:!0})==="NoopAnimations"?"di-disabled":(ew??=u(Dg).matchMedia("(prefers-reduced-motion)").matches,ew?"reduced-motion":"enabled")}function Zt(){return Ag()!=="enabled"}function We(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Cs(t){return t!=null&&`${t}`!="false"}var xn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(xn||{}),Rg=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=xn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},nw=ys({passive:!0,capture:!0}),Ng=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,nw)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,nw)))}_delegateEventHandler=n=>{let e=Ct(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},pc={enterDuration:225,exitDuration:150},sO=800,iw=ys({passive:!0,capture:!0}),rw=["mousedown","touchstart"],ow=["mouseup","mouseleave","touchend","touchcancel"],aO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),mc=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Ng;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=an(i)),o&&o.get($t).load(aO)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},pc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||cO(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),m=f.transitionProperty,g=f.transitionDuration,b=m==="none"||g==="0s"||g==="0s, 0s"||r.width===0&&r.height===0,x=new Rg(this,d,i,b);d.style.transform="scale3d(1, 1, 1)",x.state=xn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=x);let R=null;return!b&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ee=()=>{R&&(R.fallbackTimer=null),clearTimeout(Et),this._finishRippleTransition(x)},it=()=>this._destroyRipple(x),Et=setTimeout(it,l+100);d.addEventListener("transitionend",ee),d.addEventListener("transitioncancel",it),R={onTransitionEnd:ee,onTransitionCancel:it,fallbackTimer:Et}}),this._activeRipples.set(x,R),(b||!l)&&this._finishRippleTransition(x),x}fadeOutRipple(n){if(n.state===xn.FADING_OUT||n.state===xn.HIDDEN)return;let e=n.element,i=C(C({},pc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=xn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=an(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,rw.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{ow.forEach(e=>{this._triggerElement.addEventListener(e,this,iw)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===xn.FADING_IN?this._startFadeOutTransition(n):n.state===xn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=xn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=xn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=sc(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+sO;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ac(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===xn.VISIBLE||n.config.terminateOnPointerUp&&n.state===xn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(rw.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(ow.forEach(e=>n.removeEventListener(e,this,iw)),this._pointerUpEventsRegistered=!1))}};function cO(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var gc=new _("mat-ripple-global-options"),sw=(()=>{class t{_elementRef=u(z);_animationsDisabled=Zt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(O),i=u(je),r=u(gc,{optional:!0}),o=u(ce);this._globalOptions=r||{},this._rippleRenderer=new mc(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var lO={capture:!0},dO=["focus","mousedown","mouseenter","touchstart"],kg="mat-ripple-loader-uninitialized",Og="mat-ripple-loader-class-name",aw="mat-ripple-loader-centered",Vu="mat-ripple-loader-disabled",ju=(()=>{class t{_document=u(V);_animationsDisabled=Zt();_globalRippleOptions=u(gc,{optional:!0});_platform=u(je);_ngZone=u(O);_injector=u(ce);_eventCleanups;_hosts=new Map;constructor(){let e=u(Ze).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>dO.map(i=>e.listen(this._document,i,this._onInteraction,lO)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(kg,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Og))&&e.setAttribute(Og,i.className||""),i.centered&&e.setAttribute(aw,""),i.disabled&&e.setAttribute(Vu,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Vu,""):e.removeAttribute(Vu)}_onInteraction=e=>{let i=Ct(e);if(i instanceof HTMLElement){let r=i.closest(`[${kg}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Og)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??pc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??pc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Vu),rippleConfig:{centered:e.hasAttribute(aw),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new mc(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(kg)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2})}return t})();var uO=["*",[["","progressIndicator",""]]],fO=["*","[progressIndicator]"];function hO(t,n){t&1&&(jt(0,"div",1),de(1,1),Bt())}var pO=new _("MAT_BUTTON_CONFIG");function cw(t){return t==null?void 0:_n(t)}var Fg=(()=>{class t{_elementRef=u(z);_ngZone=u(O);_animationsDisabled=Zt();_config=u(pO,{optional:!0});_focusMonitor=u(dc);_cleanupClick;_renderer=u(Ge);_rippleLoader=u(ju);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Qo(!1,{transform:q});constructor(){u($t).load(co);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(pe("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Ln(r.color?"mat-"+r.color:""),ae("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",q],disabled:[2,"disabled","disabled",q],ariaDisabled:[2,"aria-disabled","ariaDisabled",q],disabledInteractive:[2,"disabledInteractive","disabledInteractive",q],tabIndex:[2,"tabIndex","tabIndex",cw],_tabindex:[2,"tabindex","_tabindex",cw],showProgress:[1,"showProgress"]}})}return t})(),lo=(()=>{class t extends Fg{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Qe],ngContentSelectors:fO,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Je(uO),yn(0,"span",0),de(1),we(2,hO,2,0,"div",1),yn(3,"span",2)(4,"span",3)),i&2&&(v(2),De(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var Es=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me]})}return t})();var mO=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],gO=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function vO(t,n){t&1&&(jt(0,"div",2),de(1,3),Bt())}var lw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Rt=(()=>{class t extends Fg{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=yO(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?lw.get(this._appearance):null,o=lw.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Qe],ngContentSelectors:gO,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Je(mO),yn(0,"span",0),de(1),jt(2,"span",1),de(3,1),Bt(),de(4,2),we(5,vO,2,0,"div",2),yn(6,"span",3)(7,"span",4)),i&2&&(ae("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),v(5),De(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function yO(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var pt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Es,Me]})}return t})();var _O=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),bO={passive:!0},dw=(()=>{class t{_platform=u(je);_ngZone=u(O);_renderer=u(Ze).createRenderer(null,null);_styleLoader=u($t);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return He;this._styleLoader.load(_O);let i=an(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new S,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,bO)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=an(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var uw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})();var fw=new _("MAT_INPUT_VALUE_ACCESSOR");var Pg=class{_box;_destroyed=new S;_resizeSubject=new S;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new te(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Te(e=>e.some(i=>i.target===n)),hl({bufferSize:1,refCount:!0}),Ve(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},hw=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(O);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new Pg(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var CO=["notch"],EO=["*"],pw=["iconPrefixContainer"],mw=["textPrefixContainer"],gw=["iconSuffixContainer"],vw=["textSuffixContainer"],wO=["textField"],DO=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],xO=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function IO(t,n){t&1&&le(0,"span",21)}function SO(t,n){if(t&1&&(p(0,"label",20),de(1,1),we(2,IO,1,0,"span",21),h()),t&2){let e=I(2);D("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),pe("for",e._control.disableAutomaticLabeling?null:e._control.id),v(2),De(!e.hideRequiredMarker&&e._control.required?2:-1)}}function MO(t,n){if(t&1&&we(0,SO,3,5,"label",20),t&2){let e=I();De(e._hasFloatingLabel()?0:-1)}}function TO(t,n){t&1&&le(0,"div",7)}function AO(t,n){}function RO(t,n){if(t&1&&W(0,AO,0,0,"ng-template",13),t&2){I(2);let e=Xi(1);D("ngTemplateOutlet",e)}}function NO(t,n){if(t&1&&(p(0,"div",9),we(1,RO,1,1,null,13),h()),t&2){let e=I();D("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),v(),De(e._forceDisplayInfixLabel()?-1:1)}}function kO(t,n){t&1&&(p(0,"div",10,2),de(2,2),h())}function OO(t,n){t&1&&(p(0,"div",11,3),de(2,3),h())}function FO(t,n){}function PO(t,n){if(t&1&&W(0,FO,0,0,"ng-template",13),t&2){I();let e=Xi(1);D("ngTemplateOutlet",e)}}function LO(t,n){t&1&&(p(0,"div",14,4),de(2,4),h())}function VO(t,n){t&1&&(p(0,"div",15,5),de(2,5),h())}function jO(t,n){t&1&&le(0,"div",16)}function BO(t,n){t&1&&(p(0,"div",18),de(1,6),h())}function UO(t,n){if(t&1&&(p(0,"mat-hint",22),y(1),h()),t&2){let e=I(2);D("id",e._hintLabelId),v(),at(e.hintLabel)}}function HO(t,n){if(t&1&&(p(0,"div",19),we(1,UO,2,2,"mat-hint",22),de(2,7),le(3,"div",23),de(4,8),h()),t&2){let e=I();v(),De(e.hintLabel?1:-1)}}var lt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-label"]]})}return t})(),zO=new _("MatError");var vc=(()=>{class t{align="start";id=u(At).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Xn("id",r.id),pe("align",null),ae("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Dw=new _("MatPrefix"),uo=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matPrefix",""],["","matIconPrefix",""],["","matTextPrefix",""]],inputs:{_isTextSelector:[0,"matTextPrefix","_isTextSelector"]},features:[et([{provide:Dw,useExisting:t}])]})}return t})(),xw=new _("MatSuffix"),cn=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[et([{provide:xw,useExisting:t}])]})}return t})(),Iw=new _("FloatingLabelParent"),yw=(()=>{class t{_elementRef=u(z);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(hw);_ngZone=u(O);_parent=u(Iw);_resizeSubscription=new ue;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return $O(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function $O(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var _w="mdc-line-ripple--active",Bu="mdc-line-ripple--deactivating",bw=(()=>{class t{_elementRef=u(z);_cleanupTransitionEnd;constructor(){let e=u(O),i=u(Ge);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Bu),e.add(_w)}deactivate(){this._elementRef.nativeElement.classList.add(Bu)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Bu);e.propertyName==="opacity"&&r&&i.remove(_w,Bu)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),Cw=(()=>{class t{_elementRef=u(z);_ngZone=u(O);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Yt(CO,5),i&2){let o;oe(o=se())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ae("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:EO,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Je(),yn(0,"div",1),jt(1,"div",2,0),de(3),Bt(),yn(4,"div",3))},encapsulation:2})}return t})(),yc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t})}return t})();var _c=new _("MatFormField"),GO=new _("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Ew="fill",WO="auto",ww="fixed",qO="translateY(-50%)",mt=(()=>{class t{_elementRef=u(z);_changeDetectorRef=u(me);_platform=u(je);_idGenerator=u(At);_ngZone=u(O);_defaults=u(GO,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Ia("iconPrefixContainer");_textPrefixContainerSignal=Ia("textPrefixContainer");_iconSuffixContainerSignal=Ia("iconSuffixContainer");_textSuffixContainerSignal=Ia("textSuffixContainer");_prefixSuffixContainers=Ut(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=N0(lt);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Cs(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||WO}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||Ew;this._appearanceSignal.set(i)}_appearanceSignal=re(Ew);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||ww}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||ww}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new S;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Zt();constructor(){let e=this._defaults,i=u(si);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Nn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Ut(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Xt([void 0,void 0]),X(()=>[i.errorState,i.userAriaDescribedBy]),fl(),Te(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ve(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Hn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){F0({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Ut(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,g=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${qO} translateX(${g}))`,x=s+a+c+l;return[b,x]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Id(o,r._labelChild,lt,5),Jn(o,yc,5)(o,Dw,5)(o,xw,5)(o,zO,5)(o,vc,5)),i&2){Md();let s;oe(s=se())&&(r._formFieldControl=s.first),oe(s=se())&&(r._prefixChildren=s),oe(s=se())&&(r._suffixChildren=s),oe(s=se())&&(r._errorChildren=s),oe(s=se())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Sd(r._iconPrefixContainerSignal,pw,5)(r._textPrefixContainerSignal,mw,5)(r._iconSuffixContainerSignal,gw,5)(r._textSuffixContainerSignal,vw,5),Yt(wO,5)(pw,5)(mw,5)(gw,5)(vw,5)(yw,5)(Cw,5)(bw,5)),i&2){Md(4);let o;oe(o=se())&&(r._textField=o.first),oe(o=se())&&(r._iconPrefixContainer=o.first),oe(o=se())&&(r._textPrefixContainer=o.first),oe(o=se())&&(r._iconSuffixContainer=o.first),oe(o=se())&&(r._textSuffixContainer=o.first),oe(o=se())&&(r._floatingLabel=o.first),oe(o=se())&&(r._notchedOutline=o.first),oe(o=se())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ae("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[et([{provide:_c,useExisting:t},{provide:Iw,useExisting:t}])],ngContentSelectors:xO,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Je(DO),W(0,MO,1,1,"ng-template",null,0,Da),p(2,"div",6,1),j("click",function(s){return r._control.onContainerClick(s)}),we(4,TO,1,0,"div",7),p(5,"div",8),we(6,NO,2,2,"div",9),we(7,kO,3,0,"div",10),we(8,OO,3,0,"div",11),p(9,"div",12),we(10,PO,1,1,null,13),de(11),h(),we(12,LO,3,0,"div",14),we(13,VO,3,0,"div",15),h(),we(14,jO,1,0,"div",16),h(),p(15,"div",17),we(16,BO,2,0,"div",18)(17,HO,5,1,"div",19),h()),i&2){let o;v(2),ae("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),v(2),De(!r._hasOutline()&&!r._control.disabled?4:-1),v(2),De(r._hasOutline()?6:-1),v(),De(r._hasIconPrefix?7:-1),v(),De(r._hasTextPrefix?8:-1),v(2),De(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),v(2),De(r._hasTextSuffix?12:-1),v(),De(r._hasIconSuffix?13:-1),v(),De(r._hasOutline()?-1:14),v(),ae("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();v(),De((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[yw,Cw,Ma,bw,vc],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var ws=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Ds=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var qe=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[GE,mt,Me]})}return t})();var YO=["button","checkbox","file","hidden","image","radio","range","reset","submit"],ZO=new _("MAT_INPUT_CONFIG"),Nt=(()=>{class t{_elementRef=u(z);_platform=u(je);ngControl=u(ii,{optional:!0,self:!0});_autofillMonitor=u(dw);_ngZone=u(O);_formField=u(_c,{optional:!0});_renderer=u(Ge);_uid=u(At).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(ZO,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new S;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Cs(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(ro.required)??!1}set required(e){this._required=Cs(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Tg().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Cs(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Tg().has(e));constructor(){let e=u(zt,{optional:!0}),i=u(oc,{optional:!0}),r=u(ws),o=u(fw,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?mn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ds(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Nn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){YO.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&j("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Xn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),pe("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ae("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",q]},exportAs:["matInput"],features:[et([{provide:yc,useExisting:t}]),Ke]})}return t})(),kt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[qe,qe,uw,Me]})}return t})();function Sw(t){return Error(`Unable to find icon with the name "${t}"`)}function KO(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function Mw(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function Tw(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Ni=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},Rw=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Ni(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(st.HTML,r);if(!s)throw Tw(r);let a=oo(s);return this._addSvgIconConfig(e,i,new Ni("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Ni(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(st.HTML,i);if(!o)throw Tw(i);let s=oo(o);return this._addSvgIconSetConfig(e,new Ni("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(st.RESOURCE_URL,e);if(!i)throw Mw(e);let r=this._cachedIconsByUrl.get(i);return r?L(Uu(r)):this._loadSvgIconFromConfig(new Ni(e,null)).pipe(ze(o=>this._cachedIconsByUrl.set(i,o)),X(o=>Uu(o)))}getNamedSvgIcon(e,i=""){let r=Aw(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Us(Sw(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?L(Uu(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(X(i=>Uu(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return L(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Ir(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(st.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),L(null)})));return zs(o).pipe(X(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw Sw(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(ze(i=>e.svgText=i),X(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?L(null):this._fetchIcon(e).pipe(ze(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(oo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(oo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw KO();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(st.RESOURCE_URL,i);if(!s)throw Mw(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(X(l=>oo(l)),Sr(()=>this._inProgressUrlFetches.delete(s)),$s());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(Aw(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return QO(o)?new Ni(o.url,null,o.options):new Ni(o,null)}}static \u0275fac=function(i){return new(i||t)(M(En,8),M(Pa),M(V,8),M(Vt))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Uu(t){return t.cloneNode(!0)}function Aw(t,n){return t+":"+n}function QO(t){return!!(t.url&&t.options)}var XO=["*"],JO=new _("MAT_ICON_DEFAULT_OPTIONS"),eF=new _("mat-icon-location",{providedIn:"root",factory:()=>{let t=u(V),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),Nw=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],tF=Nw.map(t=>`[${t}]`).join(", "),nF=/^url\(['"]?#(.*?)['"]?\)$/,Ot=(()=>{class t{_elementRef=u(z);_iconRegistry=u(Rw);_location=u(eF);_errorHandler=u(Vt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ue.EMPTY;constructor(){let e=u(new Ji("aria-hidden"),{optional:!0}),i=u(JO,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(tF),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)Nw.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(nF):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Qt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(pe("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Ln(r.color?"mat-"+r.color:""),ae("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",q],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:XO,decls:1,vars:0,template:function(i,r){i&1&&(Je(),de(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),Ft=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me]})}return t})();var Be={production:!1,apiUrl:"http://localhost:8080/tela-login-angular"};var In=class t{constructor(n){this.http=n}http;login(n,e){return this.http.post(`${Be.apiUrl}/auth/login`,{email:n,senha:e}).pipe(ze(i=>{localStorage.setItem("token",i.token),localStorage.setItem("id",i.id),localStorage.setItem("nome",i.nome),localStorage.setItem("email",i.email),localStorage.setItem("role",i.role)}))}logout(){let n=localStorage.getItem("token");localStorage.clear(),n&&this.http.post(`${Be.apiUrl}/auth/logout`,{},{headers:{Authorization:`Bearer ${n}`}}).subscribe()}getToken(){return localStorage.getItem("token")}isLoggedIn(){return!!this.getToken()}novoUsuario(n,e,i){return this.http.post(`${Be.apiUrl}/usuario`,{nome:n,email:e,senha:i})}static \u0275fac=function(e){return new(e||t)(M(En))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};function iF(t,n){if(t&1&&(p(0,"div",14),y(1),h()),t&2){let e=I();v(),xe(" ",e.mensagens," ")}}function rF(t,n){t&1&&(p(0,"mat-icon"),y(1," login "),h())}var Hu=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;email="";senha="";mensagens="";carregando=!1;onSubmit(){this.carregando=!0,this.mensagens="",this.authService.login(this.email,this.senha).subscribe({next:n=>{console.log("Login realizado com sucesso",n),this.carregando=!1,this.cdr.detectChanges(),this.router.navigate(["/pets"])},error:n=>{console.error("Falha no login",n),this.carregando=!1,n.status===401?this.mensagens=n.error?.erro??"Email ou senha invalidos.":n.status===0?this.mensagens=n.error?.erro??"Nao foi possivel conectar ao servidor.":this.mensagens=n.error?.erro??"Erro inesperado. Tente novamente.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(In),w(Le),w(me))};static \u0275cmp=N({type:t,selectors:[["app-login"]],decls:30,vars:6,consts:[[1,"login-container"],[1,"login-card"],[1,"login-header"],[1,"login-icon"],[3,"ngSubmit"],["appearance","outline"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["class","error",4,"ngIf"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[4,"ngIf"],[1,"switch-link"],["routerLink","/criar-usuario"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-icon",3),y(4," pets "),h(),p(5,"h2"),y(6,"Entrar"),h(),p(7,"p"),y(8,"Bem-vindo ao PetManager"),h()(),p(9,"form",4),j("ngSubmit",function(){return i.onSubmit()}),p(10,"mat-form-field",5)(11,"mat-label"),y(12,"Email"),h(),p(13,"input",6),Re("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),p(14,"mat-icon",7),y(15," email "),h()(),p(16,"mat-form-field",5)(17,"mat-label"),y(18,"Senha"),h(),p(19,"input",8),Re("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),p(20,"mat-icon",7),y(21," lock "),h()(),W(22,iF,2,1,"div",9),p(23,"button",10),W(24,rF,2,0,"mat-icon",11),y(25),h()(),p(26,"div",12),y(27," N\xE3o possui uma conta? "),p(28,"a",13),y(29," Criar conta "),h()()()()),e&2&&(v(13),Ae("ngModel",i.email),Fe(),v(6),Ae("ngModel",i.senha),Fe(),v(3),D("ngIf",i.mensagens),v(),D("disabled",i.carregando),v(),D("ngIf",!i.carregando),v(),xe(" ",i.carregando?"Entrando...":"Entrar"," "))},dependencies:[St,oi,ht,It,ri,Dn,bt,zt,tt,ct,nt,Tt,Mt,pt,Rt,qe,mt,lt,cn,kt,Nt,Ft,Ot],encapsulation:2})};function oF(t,n){if(t&1&&(p(0,"p"),y(1),h()),t&2){let e=I();v(),xe(" ",e.mensagens," ")}}var zu=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;nome="";email="";senha="";mensagens="";onSubmit(){this.authService.novoUsuario(this.nome,this.email,this.senha).subscribe({next:n=>{console.log("Usuario criado com sucesso",n),this.mensagens="Usuario criado com sucesso.",this.router.navigate(["/login"]),this.cdr.detectChanges()},error:n=>{console.error("Nao foi possivel criar este usuario",n),this.mensagens=n.error?.erro??"Nao foi possivel criar este usuario. Verifique se as informa\xE7\xF5es s\xE3o v\xE1lidas",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(In),w(Le),w(me))};static \u0275cmp=N({type:t,selectors:[["app-criar-usuario"]],decls:24,vars:4,consts:[[1,"login-container"],[1,"login-card"],[3,"ngSubmit"],["appearance","outline"],["matInput","","name","nome","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["mat-raised-button","","color","primary","type","submit",1,"full-width"],[4,"ngIf"],[1,"switch-link"],["routerLink","/login"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"h2"),y(3,"Criar conta"),h(),p(4,"form",2),j("ngSubmit",function(){return i.onSubmit()}),p(5,"mat-form-field",3)(6,"mat-label"),y(7,"Nome"),h(),p(8,"input",4),Re("ngModelChange",function(o){return Pe(i.nome,o)||(i.nome=o),o}),h(),Oe(),h(),p(9,"mat-form-field",3)(10,"mat-label"),y(11,"Email"),h(),p(12,"input",5),Re("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),h(),p(13,"mat-form-field",3)(14,"mat-label"),y(15,"Senha (m\xEDnimo de 4 caracteres)"),h(),p(16,"input",6),Re("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),h(),p(17,"button",7),y(18," Criar conta "),h()(),W(19,oF,2,1,"p",8),p(20,"div",9),y(21," J\xE1 possui uma conta? "),p(22,"a",10),y(23," Entrar "),h()()()()),e&2&&(v(8),Ae("ngModel",i.nome),Fe(),v(4),Ae("ngModel",i.email),Fe(),v(4),Ae("ngModel",i.senha),Fe(),v(3),D("ngIf",i.mensagens))},dependencies:[St,oi,ht,It,ri,Dn,bt,zt,tt,ct,nt,Tt,Mt,qe,mt,lt,kt,Nt,pt,Rt],styles:[".full-width[_ngcontent-%COMP%]{width:100%}.switch-link[_ngcontent-%COMP%]{margin-top:20px;text-align:center}.switch-link[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{font-weight:600}"]})};var sF=["determinateSpinner"];function aF(t,n){if(t&1&&(qi(),p(0,"svg",11),le(1,"circle",12),h()),t&2){let e=I();pe("viewBox",e._viewBox()),v(),Zr("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),pe("r",e._circleRadius())}}var cF=new _("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:kw})}),kw=100,lF=10,Sn=(()=>{class t{_elementRef=u(z);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=u(cF),i=Ag(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=kw;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-lF)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Yt(sF,5),i&2){let o;oe(o=se())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(pe("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),Ln("mat-"+r.color),Zr("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),ae("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",_n],diameter:[2,"diameter","diameter",_n],strokeWidth:[2,"strokeWidth","strokeWidth",_n]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(W(0,aF,2,8,"ng-template",null,0,Da),p(2,"div",2,1),qi(),p(4,"svg",3),le(5,"circle",4),h()(),jl(),p(6,"div",5)(7,"div",6)(8,"div",7),Ko(9,8),h(),p(10,"div",9),Ko(11,8),h(),p(12,"div",10),Ko(13,8),h()()()),i&2){let o=Xi(1);v(4),pe("viewBox",r._viewBox()),v(),Zr("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),pe("r",r._circleRadius()),v(4),D("ngTemplateOutlet",o),v(2),D("ngTemplateOutlet",o),v(2),D("ngTemplateOutlet",o)}},dependencies:[Ma],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2})}return t})();var Mn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me]})}return t})();var dF=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],uF=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function fF(t,n){t&1&&(p(0,"span",3),de(1,1),h())}function hF(t,n){t&1&&(p(0,"span",6),de(1,2),h())}var pF=["*"];var mF=new _("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),Ow=new _("MatChipAvatar"),Fw=new _("MatChipTrailingIcon"),Pw=new _("MatChipEdit"),Lw=new _("MatChipRemove"),Vw=new _("MatChip"),jw=(()=>{class t{_elementRef=u(z);_parentChip=u(Vw);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u($t).load(co),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(pe("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),ae("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:_n(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),gF=(()=>{class t extends jw{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Zn(t)))(r||t)}})();static \u0275dir=P({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&j("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(pe("tabindex",r._getTabindex()),ae("mdc-evolution-chip__action--presentational",!1))},features:[Qe]})}return t})();var Lg=(()=>{class t{_changeDetectorRef=u(me);_elementRef=u(z);_tagName=u(R0);_ngZone=u(O);_focusMonitor=u(dc);_globalRippleOptions=u(gc,{optional:!0});_document=u(V);_onFocus=new S;_onBlur=new S;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Zt();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(At).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new J;destroyed=new J;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(ju);_injector=u(ce);constructor(){let e=u($t);e.load(co),e.load(uc),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Hn(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&Jn(o,Ow,5)(o,Pw,5)(o,Fw,5)(o,Lw,5)(o,Ow,5)(o,Fw,5)(o,Pw,5)(o,Lw,5),i&2){let s;oe(s=se())&&(r.leadingIcon=s.first),oe(s=se())&&(r.editIcon=s.first),oe(s=se())&&(r.trailingIcon=s.first),oe(s=se())&&(r.removeIcon=s.first),oe(s=se())&&(r._allLeadingIcons=s),oe(s=se())&&(r._allTrailingIcons=s),oe(s=se())&&(r._allEditIcons=s),oe(s=se())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Yt(gF,5),i&2){let o;oe(o=se())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&j("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Xn("id",r.id),pe("role",r.role)("aria-label",r.ariaLabel),Ln("mat-"+(r.color||"primary")),ae("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",q],highlighted:[2,"highlighted","highlighted",q],disableRipple:[2,"disableRipple","disableRipple",q],disabled:[2,"disabled","disabled",q]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[et([{provide:Vw,useExisting:t}])],ngContentSelectors:uF,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(Je(dF),le(0,"span",0),p(1,"span",1)(2,"span",2),we(3,fF,2,0,"span",3),p(4,"span",4),de(5),le(6,"span",5),h()()(),we(7,hF,2,0,"span",6)),i&2&&(v(3),De(r.leadingIcon?3:-1),v(4),De(r._hasTrailingIcon()?7:-1))},dependencies:[jw],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return t})();var Bw=(()=>{class t{_elementRef=u(z);_changeDetectorRef=u(me);_dir=u(si,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new S;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new bi;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(Xt(null),ot(()=>Hn(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(Xt(this._chips)).subscribe(e=>{let i=[];e.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new hc(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(Ve(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(Ve(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(Xt(null),Ve(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(Ve(this._destroyed)).subscribe(e=>{let r=this._chips.toArray().indexOf(e.chip),o=e.chip._hasFocus(),s=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&Jn(o,Lg,5),i&2){let s;oe(s=se())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&j("keydown",function(s){return r._handleKeydown(s)}),i&2&&pe("role",r.role)},inputs:{disabled:[2,"disabled","disabled",q],role:"role",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:_n(e)]},ngContentSelectors:pF,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(Je(),jt(0,"div",0),de(1),Bt())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return t})();var $u=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({providers:[ws,{provide:mF,useValue:{separatorKeyCodes:[13]}}],imports:[Es,Me]})}return t})();var Is=class t{constructor(n){this.http=n}http;todos(){return this.http.get(`${Be.apiUrl}/usuario/all`)}porId(n){return this.http.get(`${Be.apiUrl}/usuario/${n}`)}porEmail(n){return this.http.get(`${Be.apiUrl}/usuario`,{params:{email:n}})}atualizar(n){return this.http.put(`${Be.apiUrl}/usuario`,n)}remover(n){return this.http.delete(`${Be.apiUrl}/usuario/${n}`)}static \u0275fac=function(e){return new(e||t)(M(En))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};var yF=t=>["/usuarios",t];function _F(t,n){if(t&1){let e=Xe();p(0,"button",10),j("click",function(){ge(e);let r=I();return r.termoBusca="",ve(r.filtrarUsuarios())}),p(1,"mat-icon"),y(2,"close"),h()()}}function bF(t,n){if(t&1&&(p(0,"div",11),y(1),h()),t&2){let e=I();v(),xe(" ",e.mensagens," ")}}function CF(t,n){t&1&&(p(0,"div",12),le(1,"mat-spinner",13),h())}function EF(t,n){if(t&1){let e=Xe();p(0,"tr")(1,"td"),y(2),h(),p(3,"td"),y(4),h(),p(5,"td")(6,"mat-chip-set")(7,"mat-chip",17),y(8),h()()(),p(9,"td",18)(10,"button",19)(11,"mat-icon"),y(12,"edit"),h()(),p(13,"button",20),j("click",function(){let r=ge(e).$implicit,o=I(2);return ve(o.remover(r.id))}),p(14,"mat-icon"),y(15,"delete"),h()()()()}if(t&2){let e=n.$implicit;v(2),at(e.nome),v(2),at(e.email),v(3),D("color",e.role==="ADMIN"?"primary":"accent"),v(),xe(" ",e.role," "),v(2),D("routerLink",Kr(5,yF,e.id))}}function wF(t,n){if(t&1&&(p(0,"table",14)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),h(),p(5,"th"),y(6,"Email"),h(),p(7,"th"),y(8,"Perfil"),h(),p(9,"th",15),y(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),W(12,EF,16,7,"tr",16),h()()),t&2){let e=I();v(12),D("ngForOf",e.usuariosFiltrados)}}function DF(t,n){t&1&&(p(0,"p"),y(1,"N\xE3o h\xE1 usu\xE1rios cadastrados."),h())}function xF(t,n){t&1&&(p(0,"p"),y(1,"Tente outro termo de busca."),h())}function IF(t,n){if(t&1&&(p(0,"div",21)(1,"mat-icon",22),y(2," group "),h(),p(3,"h3"),y(4),h(),W(5,DF,2,0,"p",23)(6,xF,2,0,"p",23),h()),t&2){let e=I();v(4),at((e.termoBusca,"Nenhum usu\xE1rio encontrado")),v(),D("ngIf",!e.termoBusca),v(),D("ngIf",e.termoBusca)}}var Gu=class t{constructor(n,e,i,r){this.usuarioService=n;this.authService=e;this.cdr=i;this.router=r}usuarioService;authService;cdr;router;usuarios=[];carregando=!0;mensagens="";usuariosFiltrados=[];termoBusca="";ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.usuarioService.todos().subscribe({next:n=>{this.usuarios=n,this.filtrarUsuarios(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar usu\xE1rios.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarUsuarios(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.usuariosFiltrados=this.usuarios;return}this.usuariosFiltrados=this.usuarios.filter(e=>e.nome?.toLowerCase().includes(n)||e.email?.toLowerCase().includes(n)||e.role?.toLowerCase().includes(n))}remover(n){if(n==null||!confirm("Confirma\xE7ao para remover este usuario"))return;let e=Number(localStorage.getItem("id"))===n;this.usuarioService.remover(n).subscribe({next:()=>{if(e){this.authService.logout(),this.router.navigate(["/login"]),this.cdr.detectChanges();return}this.usuarios=this.usuarios.filter(i=>i.id!==n),this.filtrarUsuarios(),this.cdr.detectChanges()},error:i=>{console.error("Erro ao remover usuario",i),this.mensagens="Nao foi possivel remover este usuario.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Is),w(In),w(me),w(Le))};static \u0275cmp=N({type:t,selectors:[["app-gestao-usuarios"]],decls:16,vars:6,consts:[[1,"page"],[1,"header"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, email ou perfil...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],["selected","",3,"color"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"\u{1F465} Gest\xE3o de Usu\xE1rios"),h()(),p(5,"mat-form-field",2)(6,"mat-label"),y(7,"Pesquisar"),h(),p(8,"input",3),Re("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),j("ngModelChange",function(){return i.filtrarUsuarios()}),h(),Oe(),p(9,"mat-icon",4),y(10,"search"),h(),W(11,_F,3,0,"button",5),h(),W(12,bF,2,1,"div",6)(13,CF,2,0,"div",7)(14,wF,13,1,"table",8)(15,IF,7,3,"div",9),h()()),e&2&&(v(8),Ae("ngModel",i.termoBusca),Fe(),v(3),D("ngIf",i.termoBusca),v(),D("ngIf",i.mensagens),v(),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando&&i.usuariosFiltrados.length>0),v(),D("ngIf",!i.carregando&&i.usuariosFiltrados.length===0))},dependencies:[tt,Cn,ct,St,ht,It,bt,nt,Tt,Mt,pt,lo,Ft,Ot,Mn,Sn,qe,mt,lt,uo,cn,kt,Nt,$u,Lg,Bw],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};var Ec=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new S;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var SF=20,Vg=(()=>{class t{_ngZone=u(O);_platform=u(je);_renderer=u(Ze).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new S;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=SF){return this._platform.isBrowser?new te(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(ll(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):L()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Te(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=an(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var MF=20,ho=(()=>{class t{_platform=u(je);_listeners;_viewportSize=null;_change=new S;_document=u(V);constructor(){let e=u(O),i=u(Ze).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=MF){return e>0?this._change.pipe(ll(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Wu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})(),jg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me,Wu,Me,Wu]})}return t})();var wc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Bg=class extends wc{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Dc=class extends wc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ug=class extends wc{element;constructor(n){super(),this.element=n instanceof z?n.nativeElement:n}},Hg=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Bg)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Dc)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ug)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},qu=class extends Hg{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Ei,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ce.NULL,o=r.get(Ee,i.injector);e=kd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Hw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({})}return t})();var zw=XE();function Qw(t){return new Yu(t.get(ho),t.get(V))}var Yu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=We(-this._previousScrollPosition.left),n.style.top=We(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),zw&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),zw&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function Xw(t,n){return new Zu(t.get(Vg),t.get(O),t.get(ho),n)}var Zu=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Te(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var xc=class{enable(){}disable(){}attach(){}};function zg(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function $w(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Sc(t,n){return new Ku(t.get(Vg),t.get(ho),t.get(O),n)}var Ku=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();zg(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},Jw=(()=>{class t{_injector=u(ce);noop=()=>new xc;close=e=>Xw(this._injector,e);block=()=>Qw(this._injector);reposition=e=>Sc(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Ic=class{positionStrategy;scrollStrategy=new xc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Qu=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var eD=(()=>{class t{_attachedOverlays=[];_document=u(V);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),tD=(()=>{class t extends eD{_ngZone=u(O);_renderer=u(Ze).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),nD=(()=>{class t extends eD{_platform=u(je);_ngZone=u(O);_renderer=u(Ze).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Ct(e)};_clickListener=e=>{let i=Ct(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(Gw(a.overlayElement,i)||Gw(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function Gw(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var iD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),rD=(()=>{class t{_platform=u(je);_containerElement;_document=u(V);_styleLoader=u($t);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Mg()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Mg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(iD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),$g=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Gg(t){return t&&t.nodeType===1}var Xu=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new S;_attachments=new S;_detachments=new S;_positionStrategy;_scrollStrategy;_locationChanges=ue.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new S;_outsidePointerEvents=new S;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,d=!1,f,m){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=m,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Qn(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=B(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=We(this._config.width),n.height=We(this._config.height),n.minWidth=We(this._config.minWidth),n.minHeight=We(this._config.minHeight),n.maxWidth=We(this._config.maxWidth),n.maxHeight=We(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Gg(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(n){}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new $g(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=wg(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Qn(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},Ww="cdk-overlay-connected-position-bounding-box",TF=/([A-Za-z%]+)$/;function Wg(t,n){return new Ju(n,t.get(ho),t.get(V),t.get(je),t.get(rD))}var Ju=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new S;_resizeSubscription=ue.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(Ww),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&po(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(Ww),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof z?this._origin.nativeElement:Gg(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=Yw(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,m=0-a,g=a+o.height-i.height,b=this._subtractOverflows(o.width,d,f),x=this._subtractOverflows(o.height,m,g),R=b*x;return{visibleArea:R,isCompletelyWithinViewport:o.width*o.height===R,fitsInViewportVertically:x===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=qw(this._overlayRef.getConfig().minHeight),a=qw(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=Yw(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!AF(this._lastScrollVisibility,i)){let r=new Qu(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let g=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=g*2,s=n.y-g,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-b/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,m;if(l)m=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let g=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;d=g*2,f=n.x-g,d>b&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-b/2)}return{top:s,left:f,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=We(i.width),r.height=We(i.height),r.top=We(i.top)||"auto",r.bottom=We(i.bottom)||"auto",r.left=We(i.left)||"auto",r.right=We(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=We(o)),s&&(r.maxWidth=We(s))}this._lastBoundingBoxSize=i,po(this._boundingBox.style,r)}_resetBoundingBoxStyles(){po(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){po(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();po(i,this._getExactOverlayY(e,n,d)),po(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=We(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=We(s.maxWidth):o&&(i.maxWidth="")),po(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=We(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=We(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:$w(n,i),isOriginOutsideView:zg(n,i),isOverlayClipped:$w(e,i),isOverlayOutsideView:zg(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&wg(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof z)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function po(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function qw(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(TF);return!e||e==="px"?parseFloat(n):null}return t||null}function Yw(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function AF(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var Zw="cdk-global-overlay-wrapper";function oD(t){return new ef}var ef=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(Zw),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",g="",b="",x="";c?x="flex-start":d==="center"?(x="center",m?b=f:g=f):m?d==="left"||d==="end"?(x="flex-end",g=f):(d==="right"||d==="start")&&(x="flex-start",b=f):d==="left"||d==="start"?(x="flex-start",g=f):(d==="right"||d==="end")&&(x="flex-end",b=f),n.position=this._cssPosition,n.marginLeft=c?"0":g,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":b,e.justifyContent=x,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(Zw),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},sD=(()=>{class t{_injector=u(ce);global(){return oD()}flexibleConnectedTo(e){return Wg(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Mc=new _("OVERLAY_DEFAULT_CONFIG");function qg(t,n){t.get($t).load(iD);let e=t.get(rD),i=t.get(V),r=t.get(At),o=t.get(nn),s=t.get(si),a=t.get(Ge,null,{optional:!0})||t.get(Ze).createRenderer(null,null),c=new Ic(n),l=t.get(Mc,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Gg(m)?m.after(f):m?.type==="parent"?m.element.appendChild(f):e.getContainerElement().appendChild(f),new Xu(new qu(d,o,t),f,d,c,t.get(O),t.get(tD),i,t.get(tr),t.get(nD),n?.disableAnimations??t.get(oa,null,{optional:!0})==="NoopAnimations",t.get(Ee),a)}var aD=(()=>{class t{scrollStrategies=u(Jw);_positionBuilder=u(sD);_injector=u(ce);create(e){return qg(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),RF=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],NF=new _("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ce);return()=>Sc(t)}}),Ss=(()=>{class t{elementRef=u(z);static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),cD=new _("cdk-connected-overlay-default-config"),tf=(()=>{class t{_dir=u(si,{optional:!0});_injector=u(ce);_overlayRef;_templatePortal;_backdropSubscription=ue.EMPTY;_attachSubscription=ue.EMPTY;_detachSubscription=ue.EMPTY;_positionSubscription=ue.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(NF);_ngZone=u(O);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new J;positionChange=new J;attach=new J;detach=new J;overlayKeydown=new J;overlayOutsideClick=new J;constructor(){let e=u(gn),i=u(qt),r=u(cD,{optional:!0}),o=u(Mc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Dc(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=RF);let e=this._overlayRef=qg(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!ai(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Ct(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Ic({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Wg(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ss?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ss?this.origin.elementRef.nativeElement:this.origin instanceof z?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Rf(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=P({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",q],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",q],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",q],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",q],push:[2,"cdkConnectedOverlayPush","push",q],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",q],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",q],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ke]})}return t})(),Yg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({providers:[aD],imports:[Me,Hw,jg,jg]})}return t})();var lD=(()=>{class t{_animationsDisabled=Zt();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ae("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var kF=["text"],OF=[[["mat-icon"]],"*"],FF=["mat-icon","*"];function PF(t,n){if(t&1&&le(0,"mat-pseudo-checkbox",1),t&2){let e=I();D("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function LF(t,n){if(t&1&&le(0,"mat-pseudo-checkbox",3),t&2){let e=I();D("disabled",e.disabled)}}function VF(t,n){if(t&1&&(p(0,"span",4),y(1),h()),t&2){let e=I();v(),xe("(",e.group.label,")")}}var Kg=new _("MAT_OPTION_PARENT_COMPONENT"),Qg=new _("MatOptgroup");var Zg=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},fr=(()=>{class t{_element=u(z);_changeDetectorRef=u(me);_parent=u(Kg,{optional:!0});group=u(Qg,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(At).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=re(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new J;_text;_stateChanges=new S;constructor(){let e=u($t);e.load(co),e.load(uc),this._signalDisableRipple=!!this._parent&&mn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!ai(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new Zg(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Yt(kF,7),i&2){let o;oe(o=se())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&j("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Xn("id",r.id),pe("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ae("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",q]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:FF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Je(OF),we(0,PF,1,2,"mat-pseudo-checkbox",1),de(1),p(2,"span",2,0),de(4,1),h(),we(5,LF,1,1,"mat-pseudo-checkbox",3),we(6,VF,2,1,"span",4),le(7,"div",5)),i&2&&(De(r.multiple?0:-1),v(5),De(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),v(),De(r.group&&r.group._inert?6:-1),v(),D("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[lD,sw],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function dD(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function uD(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var fD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Me]})}return t})();var Xg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Es,fD,fr,Me]})}return t})();var jF=["trigger"],BF=["panel"],UF=[[["mat-select-trigger"]],"*"],HF=["mat-select-trigger","*"];function zF(t,n){if(t&1&&(p(0,"span",4),y(1),h()),t&2){let e=I();v(),at(e.placeholder)}}function $F(t,n){t&1&&de(0)}function GF(t,n){if(t&1&&(p(0,"span",11),y(1),h()),t&2){let e=I(2);v(),at(e.triggerValue)}}function WF(t,n){if(t&1&&(p(0,"span",5),we(1,$F,1,0)(2,GF,2,1,"span",11),h()),t&2){let e=I();v(),De(e.customTrigger?1:2)}}function qF(t,n){if(t&1){let e=Xe();p(0,"div",12,1),j("keydown",function(r){ge(e);let o=I();return ve(o._handleKeydown(r))}),de(2,1),h()}if(t&2){let e=I();Ln(e.panelClass),ae("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),pe("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var YF=new _("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ce);return()=>Sc(t)}}),ZF=new _("MAT_SELECT_CONFIG"),KF=new _("MatSelectTrigger"),Jg=class{source;value;constructor(n,e){this.source=n,this.value=e}},nf=(()=>{class t{_viewportRuler=u(ho);_changeDetectorRef=u(me);_elementRef=u(z);_dir=u(si,{optional:!0});_idGenerator=u(At);_renderer=u(Ge);_parentFormField=u(_c,{optional:!0});ngControl=u(ii,{self:!0,optional:!0});_liveAnnouncer=u(xg);_defaultOptions=u(ZF,{optional:!0});_animationsDisabled=Zt();_popoverLocation;_initialized=new S;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=dD(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=uD(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Jg(this,e)}_scrollStrategyFactory=u(YF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new S;_errorStateTracker;stateChanges=new S;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=re(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(ro.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=xr(()=>{let e=this.options;return e?e.changes.pipe(Xt(e),ot(()=>Hn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(ot(()=>this.optionSelectionChanges))});openedChange=new J;_openedStream=this.openedChange.pipe(Te(e=>e),X(()=>{}));_closedStream=this.openedChange.pipe(Te(e=>!e),X(()=>{}));selectionChange=new J;valueChange=new J;constructor(){let e=u(ws),i=u(zt,{optional:!0}),r=u(oc,{optional:!0}),o=u(new Ji("tabindex"),{optional:!0}),s=u(Mc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ds(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Ec(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ve(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ve(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Xt(null),Ve(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Qt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!ai(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!ai(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!ai(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch(o){return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ss?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new fc(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Hn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ve(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Hn(...this.options.map(i=>i._stateChanges)).pipe(Ve(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Ct(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=N({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Jn(o,KF,5)(o,fr,5)(o,Qg,5),i&2){let s;oe(s=se())&&(r.customTrigger=s.first),oe(s=se())&&(r.options=s),oe(s=se())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Yt(jF,5)(BF,5)(tf,5),i&2){let o;oe(o=se())&&(r.trigger=o.first),oe(o=se())&&(r.panel=o.first),oe(o=se())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&j("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(pe("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),ae("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",q],disableRipple:[2,"disableRipple","disableRipple",q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:_n(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",q],placeholder:"placeholder",required:[2,"required","required",q],multiple:[2,"multiple","multiple",q],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",q],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",_n],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",q]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[et([{provide:yc,useExisting:t},{provide:Kg,useExisting:t}]),Ke],ngContentSelectors:HF,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Je(UF),p(0,"div",2,0),j("click",function(){return r.open()}),p(3,"div",3),we(4,zF,2,1,"span",4)(5,WF,3,1,"span",5),h(),p(6,"div",6)(7,"div",7),qi(),p(8,"svg",8),le(9,"path",9),h()()()(),W(10,qF,3,16,"ng-template",10),j("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=Xi(1);v(3),pe("id",r._valueId),v(),De(r.empty?4:5),v(6),D("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ss,tf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var Ms=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=G({type:t});static \u0275inj=$({imports:[Yg,Xg,Me,Wu,qe,Xg]})}return t})();function QF(t,n){t&1&&(p(0,"div",3),le(1,"mat-spinner",4),h())}function XF(t,n){if(t&1&&(p(0,"mat-option",17),y(1),h()),t&2){let e=n.$implicit;D("value",e),v(),xe(" ",e," ")}}function JF(t,n){if(t&1&&(p(0,"div",18),y(1),h()),t&2){let e=I(2);v(),xe(" ",e.mensagens," ")}}function e1(t,n){if(t&1){let e=Xe();p(0,"form",5),j("ngSubmit",function(){ge(e);let r=I();return ve(r.onSubmit())})("keydown.enter",function(r){ge(e);let o=I();return r.preventDefault(),ve(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),y(3,"Nome"),h(),p(4,"input",7),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.usuario.nome,r)||(o.usuario.nome=r),ve(r)}),h(),Oe(),p(5,"mat-icon",8),y(6,"person"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),y(9,"Email"),h(),p(10,"input",9),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.usuario.email,r)||(o.usuario.email=r),ve(r)}),h(),Oe(),p(11,"mat-icon",8),y(12,"email"),h()(),p(13,"mat-form-field",6)(14,"mat-label"),y(15,"Nova senha"),h(),p(16,"input",10),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.usuario.senha,r)||(o.usuario.senha=r),ve(r)}),h(),Oe(),p(17,"mat-icon",8),y(18,"lock"),h(),p(19,"mat-hint"),y(20," Deixe em branco para manter a senha atual. "),h()(),p(21,"mat-form-field",6)(22,"mat-label"),y(23,"Perfil"),h(),p(24,"mat-select",11),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.usuario.role,r)||(o.usuario.role=r),ve(r)}),W(25,XF,2,2,"mat-option",12),h(),Oe(),p(26,"mat-icon",8),y(27,"admin_panel_settings"),h()(),W(28,JF,2,1,"div",13),p(29,"div",14)(30,"button",15)(31,"mat-icon"),y(32,"save"),h(),y(33," Salvar "),h(),p(34,"button",16)(35,"mat-icon"),y(36,"arrow_back"),h(),y(37," Voltar "),h()()()}if(t&2){let e=I();v(4),Ae("ngModel",e.usuario.nome),Fe(),v(6),Ae("ngModel",e.usuario.email),Fe(),v(6),Ae("ngModel",e.usuario.senha),Fe(),v(8),Ae("ngModel",e.usuario.role),Fe(),v(),D("ngForOf",e.roles),v(3),D("ngIf",e.mensagens)}}var rf=class t{constructor(n,e,i,r,o){this.usuarioService=n;this.authService=e;this.route=i;this.router=r;this.cdr=o}usuarioService;authService;route;router;cdr;roles=["USER","ADMIN"];usuario={nome:"",email:"",senha:"",role:"USER"};carregando=!0;mensagens="";autoEdicao=!1;ngOnInit(){let n=this.route.snapshot.paramMap.get("id"),e=localStorage.getItem("id");console.log("idParam:",n),console.log("idLogado:",e);let i=localStorage.getItem("role")==="USER"||!n?e:n;console.log("id alvo:",i),i?this.usuarioService.porId(Number(i)).subscribe({next:r=>{this.usuario=B(C({},r),{senha:""}),this.autoEdicao=e!=null&&String(r.id)===e,this.carregando=!1,this.cdr.detectChanges()},error:r=>{console.error("Erro ao carregar usuario",r),this.mensagens="Usuario nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}):this.carregando=!1}onSubmit(){let n=C({},this.usuario);n.senha||delete n.senha,this.usuarioService.atualizar(n).subscribe({next:()=>{if(this.autoEdicao){this.authService.logout(),this.router.navigate(["/login"]);return}this.router.navigate(["/usuarios"])},error:e=>{console.error("Erro ao salvar usuario",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o usuario.",this.cdr.detectChanges()}}),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(w(Is),w(In),w(Ht),w(Le),w(me))};static \u0275cmp=N({type:t,selectors:[["app-usuario-form"]],decls:6,vars:2,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","email","id","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","id","senha","name","senha",3,"ngModelChange","ngModel"],["id","role","name","role",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","success",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/usuarios","type","button"],[3,"value"],[1,"success"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),y(3,"\u{1F464} Editar Usu\xE1rio"),h(),W(4,QF,2,0,"div",1)(5,e1,38,6,"form",2),h()()),e&2&&(v(4),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando))},dependencies:[tt,Cn,ct,St,oi,ht,It,ri,Dn,bt,zt,nt,Tt,Mt,qe,mt,lt,vc,cn,kt,Nt,Ms,nf,fr,pt,Rt,Ft,Ot,Mn,Sn],encapsulation:2})};var Ts=class t{constructor(n){this.http=n}http;listar(){return this.http.get(`${Be.apiUrl}/pet/all`)}porId(n){return this.http.get(`${Be.apiUrl}/pet/${n}`)}criar(n){return this.http.post(`${Be.apiUrl}/pet`,n)}atualizar(n){return this.http.put(`${Be.apiUrl}/pet`,n)}remover(n){return this.http.delete(`${Be.apiUrl}/pet/${n}`)}static \u0275fac=function(e){return new(e||t)(M(En))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};var t1=t=>["/pets",t];function n1(t,n){if(t&1){let e=Xe();p(0,"button",11),j("click",function(){ge(e);let r=I();return r.termoBusca="",ve(r.buscarPets())}),p(1,"mat-icon"),y(2,"close"),h()()}}function i1(t,n){if(t&1&&(p(0,"div",12),y(1),h()),t&2){let e=I();v(),xe(" ",e.mensagens," ")}}function r1(t,n){t&1&&(p(0,"div",13),le(1,"mat-spinner",14),h())}function o1(t,n){if(t&1){let e=Xe();p(0,"tr")(1,"td"),y(2),h(),p(3,"td"),y(4),h(),p(5,"td"),y(6),h(),p(7,"td",18)(8,"button",19)(9,"mat-icon"),y(10,"edit"),h()(),p(11,"button",20),j("click",function(){let r=ge(e).$implicit,o=I(2);return ve(o.remover(r.id))}),p(12,"mat-icon"),y(13,"delete"),h()()()()}if(t&2){let e=n.$implicit;v(2),at(e.nome),v(2),at(e.especie),v(2),at(e.dono?.email??"-"),v(2),D("routerLink",Kr(4,t1,e.id))}}function s1(t,n){if(t&1&&(p(0,"table",15)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),h(),p(5,"th"),y(6,"Esp\xE9cie"),h(),p(7,"th"),y(8,"Dono"),h(),p(9,"th",16),y(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),W(12,o1,14,6,"tr",17),h()()),t&2){let e=I();v(12),D("ngForOf",e.petsBuscados)}}function a1(t,n){t&1&&(p(0,"p"),y(1,"Clique em "),p(2,"strong"),y(3,"Novo Pet"),h(),y(4," para come\xE7ar."),h())}function c1(t,n){t&1&&(p(0,"p"),y(1,"Tente outro termo de busca."),h())}function l1(t,n){if(t&1&&(p(0,"div",21)(1,"mat-icon",22),y(2,"pets"),h(),p(3,"h3"),y(4),h(),W(5,a1,5,0,"p",23)(6,c1,2,0,"p",23),h()),t&2){let e=I();v(4),at(e.termoBusca?"Nenhum pet encontrado":"Nenhum pet cadastrado"),v(),D("ngIf",!e.termoBusca),v(),D("ngIf",e.termoBusca)}}var of=class t{constructor(n,e){this.petService=n;this.cdr=e}petService;cdr;pets=[];carregando=!0;mensagens="";termoBusca="";petsBuscados=[];ngOnInit(){this.carregar()}buscarPets(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.petsBuscados=this.pets;return}this.petsBuscados=this.pets.filter(e=>e.nome?.toLowerCase().includes(n)||e.especie?.toLowerCase().includes(n)||e.dono?.email?.toLowerCase().includes(n))}carregar(){this.carregando=!0,this.petService.listar().subscribe({next:n=>{this.pets=n,this.buscarPets(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Nao foi possivel carregar os pets.",this.carregando=!1,this.cdr.detectChanges()}})}remover(n){n!=null&&confirm("Confirma\xE7ao para remover")&&this.petService.remover(n).subscribe({next:()=>{this.pets=this.pets.filter(e=>e.id!==n),this.buscarPets(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover pet",e),this.mensagens="Nao foi possivel remover este pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Ts),w(me))};static \u0275cmp=N({type:t,selectors:[["app-gestao-pets"]],decls:20,vars:6,consts:[[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/pets/novo"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, esp\xE9cie ou dono...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"\u{1F43E} Gest\xE3o de Pets"),h(),p(5,"button",2)(6,"mat-icon"),y(7,"add"),h(),y(8," Novo Pet "),h()(),p(9,"mat-form-field",3)(10,"mat-label"),y(11,"Pesquisar"),h(),p(12,"input",4),Re("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),j("ngModelChange",function(){return i.buscarPets()}),h(),Oe(),p(13,"mat-icon",5),y(14,"search"),h(),W(15,n1,3,0,"button",6),h(),W(16,i1,2,1,"div",7)(17,r1,2,0,"div",8)(18,s1,13,1,"table",9)(19,l1,7,3,"div",10),h()()),e&2&&(v(12),Ae("ngModel",i.termoBusca),Fe(),v(3),D("ngIf",i.termoBusca),v(),D("ngIf",i.mensagens),v(),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando&&i.petsBuscados.length>0),v(),D("ngIf",!i.carregando&&i.petsBuscados.length===0))},dependencies:[tt,Cn,ct,nt,Tt,Mt,pt,Rt,lo,Ft,Ot,Mn,Sn,qe,mt,lt,uo,cn,kt,Nt,St,ht,It,bt],encapsulation:2})};function d1(t,n){t&1&&(p(0,"div",3),le(1,"mat-spinner",4),h())}function u1(t,n){if(t&1&&(p(0,"mat-option",15),y(1),h()),t&2){let e=n.$implicit;D("value",e),v(),xe(" ",e," ")}}function f1(t,n){if(t&1&&(p(0,"div",16),y(1),h()),t&2){let e=I(2);v(),xe(" ",e.mensagens," ")}}function h1(t,n){if(t&1){let e=Xe();p(0,"form",5),j("ngSubmit",function(){ge(e);let r=I();return ve(r.onSubmit())})("keydown.enter",function(r){ge(e);let o=I();return r.preventDefault(),ve(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),y(3,"Nome"),h(),p(4,"input",7),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.pet.nome,r)||(o.pet.nome=r),ve(r)}),h(),Oe(),p(5,"mat-icon",8),y(6,"pets"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),y(9,"Esp\xE9cie"),h(),p(10,"mat-select",9),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.pet.especie,r)||(o.pet.especie=r),ve(r)}),W(11,u1,2,2,"mat-option",10),h(),Oe(),p(12,"mat-icon",8),y(13,"category"),h()(),W(14,f1,2,1,"div",11),p(15,"div",12)(16,"button",13)(17,"mat-icon"),y(18,"save"),h(),y(19," Salvar "),h(),p(20,"button",14)(21,"mat-icon"),y(22,"arrow_back"),h(),y(23," Voltar "),h()()()}if(t&2){let e=I();v(4),Ae("ngModel",e.pet.nome),Fe(),v(6),Ae("ngModel",e.pet.especie),Fe(),v(),D("ngForOf",e.especies),v(3),D("ngIf",e.mensagens)}}var Tc=class t{constructor(n,e,i,r){this.petService=n;this.route=e;this.router=i;this.cdr=r}petService;route;router;cdr;especies=["CACHORRO","GATO","PEIXE","ROEDOR","AVE","OUTRA"];pet={nome:"",especie:""};modoEdicao=!1;carregando=!1;mensagens="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&(this.modoEdicao=!0,this.carregando=!0,this.petService.porId(Number(n)).subscribe({next:e=>{this.pet=e,this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar pet",e),this.mensagens="Pet nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}))}onSubmit(){(this.modoEdicao?this.petService.atualizar(this.pet):this.petService.criar(this.pet)).subscribe({next:()=>{this.router.navigate(["/pets"])},error:e=>{console.error("Erro ao salvar pet",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Ts),w(Ht),w(Le),w(me))};static \u0275cmp=N({type:t,selectors:[["app-pet-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["id","especie","name","especie","required","",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","error",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/pets","type","button"],[3,"value"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),y(3),h(),W(4,d1,2,0,"div",1)(5,h1,24,4,"form",2),h()()),e&2&&(v(3),xe(" ",i.modoEdicao?"\u270F\uFE0F Editar Pet":"\u{1F43E} Novo Pet"," "),v(),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando))},dependencies:[tt,Cn,ct,St,oi,ht,It,ri,Dn,bt,zt,nt,Tt,Mt,qe,mt,lt,cn,kt,Nt,Ms,nf,fr,pt,Rt,Ft,Ot,Mn,Sn],styles:[".actions[_ngcontent-%COMP%]{display:flex;gap:12px;margin-top:20px}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:140px}"]})};var mo=()=>{let t=u(Le);return localStorage.getItem("token")?!0:(t.navigate(["/login"]),!1)};var ev=()=>{let t=u(Le),n=localStorage.getItem("token"),e=localStorage.getItem("role");return n&&e==="ADMIN"?!0:(t.navigate(["/pets"]),!1)};var tv=()=>(localStorage.clear(),!0);var As=class t{constructor(n){this.http=n}http;todas(){return this.http.get(`${Be.apiUrl}/empresa/all`)}porId(n){return this.http.get(`${Be.apiUrl}/empresa/${n}`)}porNome(n){return this.http.get(`${Be.apiUrl}/empresa`,{params:{nome:n}})}criar(n){return this.http.post(`${Be.apiUrl}/empresa`,n)}atualizar(n){return this.http.put(`${Be.apiUrl}/empresa`,n)}remover(n){return this.http.delete(`${Be.apiUrl}/empresa/${n}`)}static \u0275fac=function(e){return new(e||t)(M(En))};static \u0275prov=F({token:t,factory:t.\u0275fac,providedIn:"root"})};var p1=t=>["/empresas",t];function m1(t,n){t&1&&(p(0,"button",11)(1,"mat-icon"),y(2,"add"),h(),y(3," Nova Empresa "),h())}function g1(t,n){if(t&1){let e=Xe();p(0,"button",12),j("click",function(){ge(e);let r=I();return r.termoBusca="",ve(r.filtrarEmpresas())}),p(1,"mat-icon"),y(2,"close"),h()()}}function v1(t,n){if(t&1&&(p(0,"div",13),y(1),h()),t&2){let e=I();v(),xe(" ",e.mensagens," ")}}function y1(t,n){t&1&&(p(0,"div",14),le(1,"mat-spinner",15),h())}function _1(t,n){if(t&1){let e=Xe();p(0,"tr")(1,"td"),y(2),h(),p(3,"td",19)(4,"button",20)(5,"mat-icon"),y(6,"edit"),h()(),p(7,"button",21),j("click",function(){let r=ge(e).$implicit,o=I(2);return ve(o.remover(r.id))}),p(8,"mat-icon"),y(9,"delete"),h()()()()}if(t&2){let e=n.$implicit;v(2),at(e.nome),v(2),D("routerLink",Kr(2,p1,e.id))}}function b1(t,n){if(t&1&&(p(0,"table",16)(1,"thead")(2,"tr")(3,"th"),y(4,"Nome"),h(),p(5,"th",17),y(6,"A\xE7\xF5es"),h()()(),p(7,"tbody"),W(8,_1,10,4,"tr",18),h()()),t&2){let e=I();v(8),D("ngForOf",e.empresasFiltradas)}}function C1(t,n){t&1&&(p(0,"p"),y(1,"Clique em "),p(2,"strong"),y(3,"Nova Empresa"),h(),y(4," para come\xE7ar."),h())}function E1(t,n){t&1&&(p(0,"p"),y(1,"Tente outro termo de busca."),h())}function w1(t,n){if(t&1&&(p(0,"div",22)(1,"mat-icon",23),y(2," group "),h(),p(3,"h3"),y(4),h(),W(5,C1,5,0,"p",24)(6,E1,2,0,"p",24),h()),t&2){let e=I();v(4),at((e.termoBusca,"Nenhuma empresa encontrada")),v(),D("ngIf",!e.termoBusca),v(),D("ngIf",e.termoBusca)}}var sf=class t{constructor(n,e){this.empresaService=n;this.cdr=e}empresaService;cdr;empresas=[];carregando=!0;mensagens="";empresasFiltradas=[];termoBusca="";get isAdmin(){return localStorage.getItem("role")==="ADMIN"}ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.empresaService.todas().subscribe({next:n=>{console.log(n),this.empresas=n,this.filtrarEmpresas(),console.log(this.empresasFiltradas),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar empresas.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarEmpresas(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.empresasFiltradas=this.empresas;return}this.empresasFiltradas=this.empresas.filter(e=>e.nome?.toLowerCase().includes(n))}remover(n){n!=null&&confirm("Confirma\xE7ao para remover esta empresa")&&this.empresaService.remover(n).subscribe({next:()=>{this.empresas=this.empresas.filter(e=>e.id!==n),this.filtrarEmpresas(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover empresa",e),this.mensagens="Nao foi possivel remover esta empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(As),w(me))};static \u0275cmp=N({type:t,selectors:[["app-gestao-empresas"]],decls:17,vars:7,consts:[[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/empresas/nova",4,"ngIf"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome da empresa...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-raised-button","","color","primary","routerLink","/empresas/nova"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),y(4,"Gest\xE3o de Empresas"),h(),W(5,m1,4,0,"button",2),h(),p(6,"mat-form-field",3)(7,"mat-label"),y(8,"Pesquisar"),h(),p(9,"input",4),Re("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),j("ngModelChange",function(){return i.filtrarEmpresas()}),h(),Oe(),p(10,"mat-icon",5),y(11,"search"),h(),W(12,g1,3,0,"button",6),h(),W(13,v1,2,1,"div",7)(14,y1,2,0,"div",8)(15,b1,9,1,"table",9)(16,w1,7,3,"div",10),h()()),e&2&&(v(5),D("ngIf",i.isAdmin),v(4),Ae("ngModel",i.termoBusca),Fe(),v(3),D("ngIf",i.termoBusca),v(),D("ngIf",i.mensagens),v(),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando&&i.empresasFiltradas.length>0),v(),D("ngIf",!i.carregando&&i.empresasFiltradas.length===0))},dependencies:[St,ht,It,bt,nt,tt,Cn,ct,Tt,Mt,pt,Rt,lo,Ft,Ot,Mn,Sn,qe,mt,lt,uo,cn,kt,Nt,$u],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};function D1(t,n){t&1&&(p(0,"div",3),le(1,"mat-spinner",4),h())}function x1(t,n){if(t&1&&(p(0,"div",12),y(1),h()),t&2){let e=I(2);v(),xe(" ",e.mensagens," ")}}function I1(t,n){if(t&1){let e=Xe();p(0,"form",5),j("ngSubmit",function(){ge(e);let r=I();return ve(r.onSubmit())})("keydown.enter",function(r){ge(e);let o=I();return r.preventDefault(),ve(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),y(3,"Nome"),h(),p(4,"input",7),Re("ngModelChange",function(r){ge(e);let o=I();return Pe(o.empresa.nome,r)||(o.empresa.nome=r),ve(r)}),h(),Oe(),h(),W(5,x1,2,1,"div",8),p(6,"div",9)(7,"button",10)(8,"mat-icon"),y(9,"save"),h(),y(10," Salvar "),h(),p(11,"button",11)(12,"mat-icon"),y(13,"arrow_back"),h(),y(14," Voltar "),h()()()}if(t&2){let e=I();v(4),Ae("ngModel",e.empresa.nome),Fe(),v(),D("ngIf",e.mensagens)}}var Ac=class t{constructor(n,e,i,r){this.empresaService=n;this.route=e;this.router=i;this.cdr=r}empresaService;route;router;cdr;empresa={nome:""};modoEdicao=!1;carregando=!1;mensagens="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&!isNaN(Number(n))&&(console.log(n),this.modoEdicao=!0,this.carregando=!0,this.empresaService.porId(Number(n)).subscribe({next:e=>{this.empresa=e,this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar empresa",e),this.mensagens="Empresa nao encontrada.",this.carregando=!1,this.cdr.detectChanges()}}))}onSubmit(){(this.modoEdicao?this.empresaService.atualizar(this.empresa):this.empresaService.criar(this.empresa)).subscribe({next:()=>{this.router.navigate(["/empresas"])},error:e=>{console.error("Erro ao salvar empresa",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar a empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(As),w(Ht),w(Le),w(me))};static \u0275cmp=N({type:t,selectors:[["app-empresa-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["class","error",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/empresas","type","button"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),y(3),h(),W(4,D1,2,0,"div",1)(5,I1,15,2,"form",2),h()()),e&2&&(v(3),xe(" ",i.modoEdicao?"\u270F\uFE0F Editar Empresa":"Nova Empresa"," "),v(),D("ngIf",i.carregando),v(),D("ngIf",!i.carregando))},dependencies:[tt,ct,St,oi,ht,It,ri,Dn,bt,zt,nt,Tt,Mt,qe,mt,lt,kt,Nt,Ms,pt,Rt,Ft,Ot,Mn,Sn],encapsulation:2})};var gD=[{path:"",redirectTo:"login",pathMatch:"full"},{path:"login",component:Hu,canActivate:[tv]},{path:"criar-usuario",component:zu,canActivate:[tv]},{path:"pets",component:of,canActivate:[mo]},{path:"pets/novo",component:Tc,canActivate:[mo]},{path:"pets/:id",component:Tc,canActivate:[mo]},{path:"usuarios",component:Gu,canActivate:[mo]},{path:"usuarios/:id",component:rf,canActivate:[mo]},{path:"empresas",component:sf,canActivate:[mo]},{path:"empresas/novo",component:Ac,canActivate:[ev]},{path:"empresas/:id",component:Ac,canActivate:[ev]}];var vD=(t,n)=>{let e=localStorage.getItem("token");return e&&(t=t.clone({setHeaders:{Authorization:`Bearer ${e}`}})),n(t)};var yD={providers:[Ah(),lg(gD),Pm(Lm([vD]))]};function S1(t,n){if(t&1&&(p(0,"span",11)(1,"mat-icon"),y(2,"account_circle"),h(),y(3),h()),t&2){let e=I(2);v(3),xe(" ",e.emailUsuario," ")}}function M1(t,n){if(t&1){let e=Xe();p(0,"nav",1)(1,"div",2),le(2,"span",3),h(),p(3,"div",4)(4,"a",5),y(5,"Pets"),h(),p(6,"a",6),y(7,"Usu\xE1rios"),h(),p(8,"a",7),y(9,"Empresas"),h()(),p(10,"div",8),W(11,S1,4,1,"span",9),p(12,"button",10),j("click",function(){ge(e);let r=I();return ve(r.sair())}),p(13,"mat-icon"),y(14,"logout"),h(),y(15," Sair "),h()()()}if(t&2){let e=I();v(11),D("ngIf",e.logado)}}var af=class t{constructor(n,e){this.authService=n;this.router=e}authService;router;get logado(){return this.authService.isLoggedIn()}get emailUsuario(){return localStorage.getItem("email")}sair(){this.authService.logout(),this.router.navigate(["/login"])}get isAdmin(){return localStorage.getItem("role")==="ADMIN"}static \u0275fac=function(e){return new(e||t)(w(In),w(Le))};static \u0275cmp=N({type:t,selectors:[["app-root"]],decls:2,vars:1,consts:[["class","top-nav",4,"ngIf"],[1,"top-nav"],[1,"nav-left"],[1,"logo"],[1,"nav-center"],["routerLink","/pets","routerLinkActive","active"],["routerLink","/usuarios","routerLinkActive","active"],["routerLink","/empresas","routerLinkActive","active"],[1,"nav-right"],["class","nav-user",4,"ngIf"],["mat-raised-button","","color","primary",3,"click"],[1,"nav-user"]],template:function(e,i){e&1&&(W(0,M1,16,1,"nav",0),le(1,"router-outlet")),e&2&&D("ngIf",i.logado)},dependencies:[Qa,tt,ct,nt,Ft,Ot,pt,Rt],encapsulation:2})};Sm(af,yD).catch(t=>console.error(t));
