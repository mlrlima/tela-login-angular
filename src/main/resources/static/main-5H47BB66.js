var FD=Object.defineProperty,PD=Object.defineProperties;var LD=Object.getOwnPropertyDescriptors;var Dv=Object.getOwnPropertySymbols;var VD=Object.prototype.hasOwnProperty,jD=Object.prototype.propertyIsEnumerable;var xv=(t,n,e)=>n in t?FD(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})VD.call(n,e)&&xv(t,e,n[e]);if(Dv)for(var e of Dv(n))jD.call(n,e)&&xv(t,e,n[e]);return t},z=(t,n)=>PD(t,LD(n));var De=(t,n,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(t,n)).next())});var $t=null,Zc=!1,xr=1,BD=null,qe=Symbol("SIGNAL");function G(t){let n=$t;return $t=t,n}function Kc(){return $t}var ji={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Bi(t){if(Zc)throw new Error("");if($t===null)return;$t.consumerOnSignalRead(t);let n=$t.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=$t.recomputing;if(i&&(e=n!==void 0?n.nextProducer:$t.producers,e!==void 0&&e.producer===t)){$t.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=xr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===$t&&(!i||r.knownValidAtEpoch===xr))return;let o=ko($t),s={producer:t,consumer:$t,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:xr,lastReadVersion:t.version,nextConsumer:void 0};$t.producersTail=s,n!==void 0?n.nextProducer=s:$t.producers=s,o&&Tv(t,s)}function Sv(){xr++}function Mr(t){if(!(ko(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===xr)){if(!t.producerMustRecompute(t)&&!No(t)){Ro(t);return}t.producerRecomputeValue(t),Ro(t)}}function Of(t){if(t.consumers===void 0)return;let n=Zc;Zc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||UD(i)}}finally{Zc=n}}function Ff(){return $t?.consumerAllowSignalWrites!==!1}function UD(t){t.dirty=!0,Of(t),t.consumerMarkedDirty?.(t)}function Ro(t){t.dirty=!1,t.lastCleanEpoch=xr}function hi(t){return t&&Iv(t),G(t)}function Iv(t){if(t.producersTail?.knownValidAtEpoch===xr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Ui(t,n){G(n),t&&Mv(t)}function Mv(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(ko(t))do e=Pf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function No(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Mr(e),i!==e.version))return!0}return!1}function Hi(t){if(ko(t)){let n=t.producers;for(;n!==void 0;)n=Pf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Tv(t,n){let e=t.consumersTail,i=ko(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Tv(r.producer,r)}function Pf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!ko(n)){let o=n.producers;for(;o!==void 0;)o=Pf(o)}return e}function ko(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function qs(t){BD?.(t)}function Ys(t,n){return Object.is(t,n)}function Zs(t,n){let e=Object.create(HD);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Mr(e),Bi(e),e.value===Kn)throw e.error;return e.value};return i[qe]=e,qs(e),i}var Sr=Symbol("UNSET"),Ir=Symbol("COMPUTING"),Kn=Symbol("ERRORED"),HD=z(C({},ji),{value:Sr,dirty:!0,error:null,equal:Ys,kind:"computed",producerMustRecompute(t){return t.value===Sr||t.value===Ir},producerRecomputeValue(t){if(t.value===Ir)throw new Error("");let n=t.value;t.value=Ir;let e=hi(t),i,r=!1;try{i=t.computation(),G(null),r=n!==Sr&&n!==Kn&&i!==Kn&&t.equal(n,i)}catch(o){i=Kn,t.error=o}finally{Ui(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function zD(){throw new Error}var Av=zD;function Rv(t){Av(t)}function Lf(t){Av=t}var $D=null;function Vf(t,n){let e=Object.create(Ks);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Nv(e);return i[qe]=e,qs(e),[i,s=>Tr(e,s),s=>Qc(e,s)]}function Nv(t){return Bi(t),t.value}function Tr(t,n){Ff()||Rv(t),t.equal(t.value,n)||(t.value=n,GD(t))}function Qc(t,n){Ff()||Rv(t),Tr(t,n(t.value))}var Ks=z(C({},ji),{equal:Ys,value:void 0,kind:"signal"});function GD(t){t.version++,Sv(),Of(t),$D?.(t)}var jf=z(C({},ji),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Bf(t){if(t.dirty=!1,t.version>0&&!No(t))return;t.version++;let n=hi(t);try{t.cleanup(),t.fn()}finally{Ui(t,n)}}var Uf;function Xc(){return Uf}function Qn(t){let n=Uf;return Uf=t,n}var kv=Symbol("NotFound");function Oo(t){return t===kv||t?.name==="\u0275NotFound"}function Hf(t,n,e){let i=Object.create(WD);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Mr(i),Bi(i),i.value===Kn)throw i.error;return i.value};return o[qe]=i,qs(i),o}function Ov(t,n){Mr(t),Tr(t,n),Ro(t)}function Fv(t,n){if(Mr(t),t.value===Kn)throw t.error;Qc(t,n),Ro(t)}var WD=z(C({},ji),{value:Sr,dirty:!0,error:null,equal:Ys,kind:"linkedSignal",producerMustRecompute(t){return t.value===Sr||t.value===Ir},producerRecomputeValue(t){if(t.value===Ir)throw new Error("");let n=t.value;t.value=Ir;let e=hi(t),i,r=!1;try{let o=t.source(),s=n!==Sr&&n!==Kn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,G(null),r=s&&i!==Kn&&t.equal(n,i)}catch(o){i=Kn,t.error=o}finally{Ui(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Pv(t){let n=G(null);try{return t()}finally{G(n)}}function J(t){return typeof t=="function"}function Fo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Jc=Fo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Ar(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var fe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(J(i))try{i()}catch(o){n=o instanceof Jc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Lv(o)}catch(s){n=n??[],s instanceof Jc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Jc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Lv(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Ar(e,n)}remove(n){let{_finalizers:e}=this;e&&Ar(e,n),n instanceof t&&n._removeParent(this)}};fe.EMPTY=(()=>{let t=new fe;return t.closed=!0,t})();var zf=fe.EMPTY;function el(t){return t instanceof fe||t&&"closed"in t&&J(t.remove)&&J(t.add)&&J(t.unsubscribe)}function Lv(t){J(t)?t():t.unsubscribe()}var Vn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Po={setTimeout(t,n,...e){let{delegate:i}=Po;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Po;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function tl(t){Po.setTimeout(()=>{let{onUnhandledError:n}=Vn;if(n)n(t);else throw t})}function Qs(){}var Vv=$f("C",void 0,void 0);function jv(t){return $f("E",void 0,t)}function Bv(t){return $f("N",t,void 0)}function $f(t,n,e){return{kind:t,value:n,error:e}}var Rr=null;function Lo(t){if(Vn.useDeprecatedSynchronousErrorHandling){let n=!Rr;if(n&&(Rr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Rr;if(Rr=null,e)throw i}}else t()}function Uv(t){Vn.useDeprecatedSynchronousErrorHandling&&Rr&&(Rr.errorThrown=!0,Rr.error=t)}var Nr=class extends fe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,el(n)&&n.add(this)):this.destination=ZD}static create(n,e,i){return new pi(n,e,i)}next(n){this.isStopped?Wf(Bv(n),this):this._next(n)}error(n){this.isStopped?Wf(jv(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Wf(Vv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},qD=Function.prototype.bind;function Gf(t,n){return qD.call(t,n)}var qf=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){nl(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){nl(i)}else nl(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){nl(e)}}},pi=class extends Nr{constructor(n,e,i){super();let r;if(J(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Vn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Gf(n.next,o),error:n.error&&Gf(n.error,o),complete:n.complete&&Gf(n.complete,o)}):r=n}this.destination=new qf(r)}};function nl(t){Vn.useDeprecatedSynchronousErrorHandling?Uv(t):tl(t)}function YD(t){throw t}function Wf(t,n){let{onStoppedNotification:e}=Vn;e&&Po.setTimeout(()=>e(t,n))}var ZD={closed:!0,next:Qs,error:YD,complete:Qs};var Vo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function rn(t){return t}function Yf(...t){return Zf(t)}function Zf(t){return t.length===0?rn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var re=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=QD(e)?e:new pi(e,i,r);return Lo(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Hv(i),new i((r,o)=>{let s=new pi({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Vo](){return this}pipe(...e){return Zf(e)(this)}toPromise(e){return e=Hv(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Hv(t){var n;return(n=t??Vn.Promise)!==null&&n!==void 0?n:Promise}function KD(t){return t&&J(t.next)&&J(t.error)&&J(t.complete)}function QD(t){return t&&t instanceof Nr||KD(t)&&el(t)}function XD(t){return J(t?.lift)}function te(t){return n=>{if(XD(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function oe(t,n,e,i,r){return new Kf(t,n,e,i,r)}var Kf=class extends Nr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var zv=Fo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var I=(()=>{class t extends re{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new il(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new zv}next(e){Lo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Lo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Lo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?zf:(this.currentObservers=null,o.push(e),new fe(()=>{this.currentObservers=null,Ar(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new re;return e.source=this,e}}return t.create=(n,e)=>new il(n,e),t})(),il=class extends I{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:zf}};var at=class extends I{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Xs={now(){return(Xs.delegate||Date).now()},delegate:void 0};var rl=class extends I{constructor(n=1/0,e=1/0,i=Xs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var ol=class extends fe{constructor(n,e){super()}schedule(n,e=0){return this}};var Js={setInterval(t,n,...e){let{delegate:i}=Js;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Js;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var sl=class extends ol{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Js.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Js.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Ar(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var jo=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};jo.now=Xs.now;var al=class extends jo{constructor(n,e=jo.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var ea=new al(sl),$v=ea;var Ye=new re(t=>t.complete());function cl(t){return t&&J(t.schedule)}function Qf(t){return t[t.length-1]}function ll(t){return J(Qf(t))?t.pop():void 0}function Xn(t){return cl(Qf(t))?t.pop():void 0}function Gv(t,n){return typeof Qf(t)=="number"?t.pop():n}function qv(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function Wv(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function kr(t){return this instanceof kr?(this.v=t,this):new kr(t)}function Yv(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(g){return function(b){return Promise.resolve(b).then(g,f)}}function a(g,b){i[g]&&(r[g]=function(S){return new Promise(function(N,ie){o.push([g,S,N,ie])>1||c(g,S)})},b&&(r[g]=b(r[g])))}function c(g,b){try{l(i[g](b))}catch(S){m(o[0][3],S)}}function l(g){g.value instanceof kr?Promise.resolve(g.value.v).then(d,f):m(o[0][2],g)}function d(g){c("next",g)}function f(g){c("throw",g)}function m(g,b){g(b),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Zv(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Wv=="function"?Wv(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var dl=t=>t&&typeof t.length=="number"&&typeof t!="function";function ul(t){return J(t?.then)}function fl(t){return J(t[Vo])}function hl(t){return Symbol.asyncIterator&&J(t?.[Symbol.asyncIterator])}function pl(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function JD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ml=JD();function gl(t){return J(t?.[ml])}function vl(t){return Yv(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield kr(e.read());if(r)return yield kr(void 0);yield yield kr(i)}}finally{e.releaseLock()}})}function _l(t){return J(t?.getReader)}function Te(t){if(t instanceof re)return t;if(t!=null){if(fl(t))return ex(t);if(dl(t))return tx(t);if(ul(t))return nx(t);if(hl(t))return Kv(t);if(gl(t))return ix(t);if(_l(t))return rx(t)}throw pl(t)}function ex(t){return new re(n=>{let e=t[Vo]();if(J(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function tx(t){return new re(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function nx(t){return new re(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,tl)})}function ix(t){return new re(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Kv(t){return new re(n=>{ox(t,n).catch(e=>n.error(e))})}function rx(t){return Kv(vl(t))}function ox(t,n){var e,i,r,o;return qv(this,void 0,void 0,function*(){try{for(e=Zv(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Qt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function yl(t,n=0){return te((e,i)=>{e.subscribe(oe(i,r=>Qt(i,t,()=>i.next(r),n),()=>Qt(i,t,()=>i.complete(),n),r=>Qt(i,t,()=>i.error(r),n)))})}function bl(t,n=0){return te((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Qv(t,n){return Te(t).pipe(bl(n),yl(n))}function Xv(t,n){return Te(t).pipe(bl(n),yl(n))}function Jv(t,n){return new re(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function e_(t,n){return new re(e=>{let i;return Qt(e,n,()=>{i=t[ml](),Qt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>J(i?.return)&&i.return()})}function Cl(t,n){if(!t)throw new Error("Iterable cannot be null");return new re(e=>{Qt(e,n,()=>{let i=t[Symbol.asyncIterator]();Qt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function t_(t,n){return Cl(vl(t),n)}function n_(t,n){if(t!=null){if(fl(t))return Qv(t,n);if(dl(t))return Jv(t,n);if(ul(t))return Xv(t,n);if(hl(t))return Cl(t,n);if(gl(t))return e_(t,n);if(_l(t))return t_(t,n)}throw pl(t)}function Le(t,n){return n?n_(t,n):Te(t)}function V(...t){let n=Xn(t);return Le(t,n)}function ta(t,n){let e=J(t)?t:()=>t,i=r=>r.error(e());return new re(n?r=>n.schedule(i,0,r):i)}function na(t){return!!t&&(t instanceof re||J(t.lift)&&J(t.subscribe))}var Or=Fo(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function i_(t){return t instanceof Date&&!isNaN(t)}function ne(t,n){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:sx}=Array;function ax(t,n){return sx(n)?t(...n):t(n)}function El(t){return ne(n=>ax(t,n))}var{isArray:cx}=Array,{getPrototypeOf:lx,prototype:dx,keys:ux}=Object;function wl(t){if(t.length===1){let n=t[0];if(cx(n))return{args:n,keys:null};if(fx(n)){let e=ux(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function fx(t){return t&&typeof t=="object"&&lx(t)===dx}function Dl(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Xf(...t){let n=Xn(t),e=ll(t),{args:i,keys:r}=wl(t);if(i.length===0)return Le([],n);let o=new re(hx(i,n,r?s=>Dl(r,s):rn));return e?o.pipe(El(e)):o}function hx(t,n,e=rn){return i=>{r_(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)r_(n,()=>{let l=Le(t[c],n),d=!1;l.subscribe(oe(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function r_(t,n,e){t?Qt(e,t,n):n()}function o_(t,n,e,i,r,o,s,a){let c=[],l=0,d=0,f=!1,m=()=>{f&&!c.length&&!l&&n.complete()},g=S=>l<i?b(S):c.push(S),b=S=>{o&&n.next(S),l++;let N=!1;Te(e(S,d++)).subscribe(oe(n,ie=>{r?.(ie),o?g(ie):n.next(ie)},()=>{N=!0},void 0,()=>{if(N)try{for(l--;c.length&&l<i;){let ie=c.shift();s?Qt(n,s,()=>b(ie)):b(ie)}m()}catch(ie){n.error(ie)}}))};return t.subscribe(oe(n,g,()=>{f=!0,m()})),()=>{a?.()}}function Gt(t,n,e=1/0){return J(n)?Gt((i,r)=>ne((o,s)=>n(i,o,r,s))(Te(t(i,r))),e):(typeof n=="number"&&(e=n),te((i,r)=>o_(i,r,t,e)))}function xl(t=1/0){return Gt(rn,t)}function s_(){return xl(1)}function Bo(...t){return s_()(Le(t,Xn(t)))}function Fr(t){return new re(n=>{Te(t()).subscribe(n)})}function ia(...t){let n=ll(t),{args:e,keys:i}=wl(t),r=new re(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;Te(e[d]).subscribe(oe(o,m=>{f||(f=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?Dl(i,a):a),o.complete())}))}});return n?r.pipe(El(n)):r}function a_(t=0,n,e=$v){let i=-1;return n!=null&&(cl(n)?e=n:i=n),new re(r=>{let o=i_(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Xt(...t){let n=Xn(t),e=Gv(t,1/0),i=t;return i.length?i.length===1?Te(i[0]):xl(e)(Le(i,n)):Ye}function Ie(t,n){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function c_(t){return te((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(oe(e,l=>{i=!0,r=l,o||Te(t(l)).subscribe(o=oe(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Sl(t,n=ea){return c_(()=>a_(t,n))}function Pr(t){return te((n,e)=>{let i=null,r=!1,o;i=n.subscribe(oe(e,void 0,void 0,s=>{o=Te(t(s,Pr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Uo(t,n){return J(n)?Gt(t,n,1):Gt(t,1)}function Jf(t,n=ea){return te((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}e.subscribe(oe(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function l_(t){return te((n,e)=>{let i=!1;n.subscribe(oe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Wt(t){return t<=0?()=>Ye:te((n,e)=>{let i=0;n.subscribe(oe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Il(t,n=rn){return t=t??px,te((e,i)=>{let r,o=!0;e.subscribe(oe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function px(t,n){return t===n}function d_(t=mx){return te((n,e)=>{let i=!1;n.subscribe(oe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function mx(){return new Or}function Lr(t){return te((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function mi(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Ie((r,o)=>t(r,o,i)):rn,Wt(1),e?l_(n):d_(()=>new Or))}function Ml(t){return t<=0?()=>Ye:te((n,e)=>{let i=[];n.subscribe(oe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Tl(){return te((t,n)=>{let e,i=!1;t.subscribe(oe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function ra(t={}){let{connector:n=()=>new I,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,d=!1,f=!1,m=()=>{a?.unsubscribe(),a=void 0},g=()=>{m(),s=c=void 0,d=f=!1},b=()=>{let S=s;g(),S?.unsubscribe()};return te((S,N)=>{l++,!f&&!d&&m();let ie=c=c??n();N.add(()=>{l--,l===0&&!f&&!d&&(a=eh(b,r))}),ie.subscribe(N),!s&&l>0&&(s=new pi({next:st=>ie.next(st),error:st=>{f=!0,m(),a=eh(g,e,st),ie.error(st)},complete:()=>{d=!0,m(),a=eh(g,i),ie.complete()}}),Te(S).subscribe(s))})(o)}}function eh(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new pi({next:()=>{i.unsubscribe(),t()}});return Te(n(...e)).subscribe(i)}function Al(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,ra({connector:()=>new rl(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function th(t){return Ie((n,e)=>t<=e)}function yt(...t){let n=Xn(t);return te((e,i)=>{(n?Bo(t,e,n):Bo(t,e)).subscribe(i)})}function We(t,n){return te((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(oe(i,c=>{r?.unsubscribe();let l=0,d=o++;Te(t(c,d)).subscribe(r=oe(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function ke(t){return te((n,e)=>{Te(t).subscribe(oe(e,()=>e.complete(),Qs)),!e.closed&&n.subscribe(e)})}function nh(t,n=!1){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function Ze(t,n,e){let i=J(t)||n||e?{next:t,error:n,complete:e}:t;return i?te((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(oe(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):rn}var Ll="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(n,e){super(_i(n,e)),this.code=n}};function gx(t){return`NG0${Math.abs(t)}`}function _i(t,n){return`${gx(t)}${n?": "+n:""}`}function Ee(t){for(let n in t)if(t[n]===Ee)return n;throw Error("")}function m_(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function da(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(da).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Vl(t,n){return t?n?`${t} ${n}`:t:n||""}var vx=Ee({__forward_ref__:Ee});function pn(t){return t.__forward_ref__=pn,t}function bt(t){return mh(t)?t():t}function mh(t){return typeof t=="function"&&t.hasOwnProperty(vx)&&t.__forward_ref__===pn}function P(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function $(t){return{providers:t.providers||[],imports:t.imports||[]}}function ua(t){return _x(t,jl)}function gh(t){return ua(t)!==null}function _x(t,n){return t.hasOwnProperty(n)&&t[n]||null}function yx(t){let n=t?.[jl]??null;return n||null}function rh(t){return t&&t.hasOwnProperty(Nl)?t[Nl]:null}var jl=Ee({\u0275prov:Ee}),Nl=Ee({\u0275inj:Ee}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=P({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function vh(t){return t&&!!t.\u0275providers}var _h=Ee({\u0275cmp:Ee}),yh=Ee({\u0275dir:Ee}),bh=Ee({\u0275pipe:Ee}),Ch=Ee({\u0275mod:Ee}),sa=Ee({\u0275fac:Ee}),zr=Ee({__NG_ELEMENT_ID__:Ee}),u_=Ee({__NG_ENV_ID__:Ee});function g_(t){return Bl(t,"@NgModule"),t[Ch]||null}function $i(t){return Bl(t,"@Component"),t[_h]||null}function Eh(t){return Bl(t,"@Directive"),t[yh]||null}function v_(t){return Bl(t,"@Pipe"),t[bh]||null}function Bl(t,n){if(t==null)throw new x(-919,!1)}function Ul(t){return typeof t=="string"?t:t==null?"":String(t)}var __=Ee({ngErrorCode:Ee}),bx=Ee({ngErrorMessage:Ee}),Cx=Ee({ngTokenPath:Ee});function wh(t,n){return y_("",-200,n)}function Hl(t,n){throw new x(-201,!1)}function y_(t,n,e){let i=new x(n,t);return i[__]=n,i[bx]=t,e&&(i[Cx]=e),i}function Ex(t){return t[__]}var oh;function b_(){return oh}function on(t){let n=oh;return oh=t,n}function Dh(t,n,e){let i=ua(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Hl(t,"")}var Gi=globalThis;var wx={},Vr=wx,Dx="__NG_DI_FLAG__",sh=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=jr(e)||0;try{return this.injector.get(n,i&8?null:Vr,i)}catch(r){if(Oo(r))return r;throw r}}};function xx(t,n=0){let e=Xc();if(e===void 0)throw new x(-203,!1);if(e===null)return Dh(t,void 0,n);{let i=Sx(n),r=e.retrieve(t,i);if(Oo(r)){if(i.optional)return null;throw r}return r}}function M(t,n=0){return(b_()||xx)(bt(t),n)}function u(t,n){return M(t,jr(n))}function jr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function Sx(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function ah(t){let n=[];for(let e=0;e<t.length;e++){let i=bt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new x(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=Ix(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(M(r,o))}else n.push(M(i))}return n}function Ix(t){return t[Dx]}function Br(t,n){let e=t.hasOwnProperty(sa);return e?t[sa]:null}function C_(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function E_(t){return t.flat(Number.POSITIVE_INFINITY)}function zl(t,n){t.forEach(e=>Array.isArray(e)?zl(e,n):n(e))}function xh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function fa(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function w_(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function D_(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function $l(t,n,e){let i=$o(t,n);return i>=0?t[i|1]=e:(i=~i,D_(t,i,n,e)),i}function Gl(t,n){let e=$o(t,n);if(e>=0)return t[e|1]}function $o(t,n){return Mx(t,n,1)}function Mx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Wi={},At=[],$r=new y(""),ha=new y("",-1),Sh=new y(""),zo=class{get(n,e=Vr){if(e===Vr){let r=y_("",-201);throw r.name="\u0275NotFound",r}return e}};function yi(t){return{\u0275providers:t}}function x_(t){return yi([{provide:$r,multi:!0,useValue:t}])}function S_(...t){return{\u0275providers:Ih(!0,t),\u0275fromNgModule:!0}}function Ih(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return zl(n,s=>{let a=s;kl(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&I_(r,o),e}function I_(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Mh(r,o=>{n(o,i)})}}function kl(t,n,e,i){if(t=bt(t),!t)return!1;let r=null,o=rh(t),s=!o&&$i(t);if(!o&&!s){let c=t.ngModule;if(o=rh(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)kl(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;zl(o.imports,d=>{kl(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&I_(l,n)}if(!a){let l=Br(r)||(()=>new r);n({provide:r,useFactory:l,deps:At},r),n({provide:Sh,useValue:r,multi:!0},r),n({provide:$r,useValue:()=>M(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;Mh(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function Mh(t,n){for(let e of t)vh(e)&&(e=e.\u0275providers),Array.isArray(e)?Mh(e,n):n(e)}var Tx=Ee({provide:String,useValue:Ee});function M_(t){return t!==null&&typeof t=="object"&&Tx in t}function Ax(t){return!!(t&&t.useExisting)}function Rx(t){return!!(t&&t.useFactory)}function Ur(t){return typeof t=="function"}function T_(t){return!!t.useClass}var pa=new y(""),Rl={},f_={},ih;function Go(){return ih===void 0&&(ih=new zo),ih}var xe=class{},Hr=class extends xe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,lh(n,s=>this.processProvider(s)),this.records.set(ha,Ho(void 0,this)),r.has("environment")&&this.records.set(xe,Ho(void 0,this));let o=this.records.get(pa);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Sh,At,{self:!0}))}retrieve(n,e){let i=jr(e)||0;try{return this.get(n,Vr,i)}catch(r){if(Oo(r))return r;throw r}}destroy(){oa(this),this._destroyed=!0;let n=G(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),G(n)}}onDestroy(n){return oa(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){oa(this);let e=Qn(this),i=on(void 0),r;try{return n()}finally{Qn(e),on(i)}}get(n,e=Vr,i){if(oa(this),n.hasOwnProperty(u_))return n[u_](this);let r=jr(i),o,s=Qn(this),a=on(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=Px(n)&&ua(n);d&&this.injectableDefInScope(d)?l=Ho(ch(n),Rl):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Go():this.parent;return e=r&8&&e===Vr?null:e,c.get(n,e)}catch(c){let l=Ex(c);throw l===-200||l===-201?new x(l,null):c}finally{on(a),Qn(s)}}resolveInjectorInitializers(){let n=G(null),e=Qn(this),i=on(void 0),r;try{let o=this.get($r,At,{self:!0});for(let s of o)s()}finally{Qn(e),on(i),G(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=bt(n);let e=Ur(n)?n:bt(n&&n.provide),i=kx(n);if(!Ur(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Ho(void 0,Rl,!0),r.factory=()=>ah(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=G(null);try{if(e.value===f_)throw wh("");return e.value===Rl&&(e.value=f_,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Fx(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{G(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=bt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function ch(t){let n=ua(t),e=n!==null?n.factory:Br(t);if(e!==null)return e;if(t instanceof y)throw new x(-204,!1);if(t instanceof Function)return Nx(t);throw new x(-204,!1)}function Nx(t){if(t.length>0)throw new x(-204,!1);let e=yx(t);return e!==null?()=>e.factory(t):()=>new t}function kx(t){if(M_(t))return Ho(void 0,t.useValue);{let n=Th(t);return Ho(n,Rl)}}function Th(t,n,e){let i;if(Ur(t)){let r=bt(t);return Br(r)||ch(r)}else if(M_(t))i=()=>bt(t.useValue);else if(Rx(t))i=()=>t.useFactory(...ah(t.deps||[]));else if(Ax(t))i=(r,o)=>M(bt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=bt(t&&(t.useClass||t.provide));if(Ox(t))i=()=>new r(...ah(t.deps));else return Br(r)||ch(r)}return i}function oa(t){if(t.destroyed)throw new x(-205,!1)}function Ho(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function Ox(t){return!!t.deps}function Fx(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function Px(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function lh(t,n){for(let e of t)Array.isArray(e)?lh(e,n):e&&vh(e)?lh(e.\u0275providers,n):n(e)}function ft(t,n){let e;t instanceof Hr?(oa(t),e=t):e=new sh(t);let i,r=Qn(e),o=on(void 0);try{return n()}finally{Qn(r),on(o)}}function A_(){return b_()!==void 0||Xc()!=null}var jn=0,L=1,W=2,ut=3,mn=4,Rt=5,Wo=6,qo=7,Ct=8,bi=9,Bn=10,Me=11,Yo=12,Ah=13,qi=14,Jt=15,Yi=16,Gr=17,ei=18,Ci=19,Rh=20,gi=21,Wl=22,zi=23,sn=24,Wr=25,Zi=26,Ke=27,R_=1;var Ki=7,ma=8,qr=9,Et=10;function Ei(t){return Array.isArray(t)&&typeof t[R_]=="object"}function gn(t){return Array.isArray(t)&&t[R_]===!0}function Nh(t){return(t.flags&4)!==0}function wi(t){return t.componentOffset>-1}function Zo(t){return(t.flags&1)===1}function ti(t){return!!t.template}function Ko(t){return(t[W]&512)!==0}function Yr(t){return(t[W]&256)===256}var kh="svg",N_="math";function vn(t){for(;Array.isArray(t);)t=t[jn];return t}function Oh(t,n){return vn(n[t])}function _n(t,n){return vn(n[t.index])}function ql(t,n){return t.data[n]}function k_(t,n){return t[n]}function yn(t,n){let e=n[t];return Ei(e)?e:e[jn]}function O_(t){return(t[W]&4)===4}function Yl(t){return(t[W]&128)===128}function F_(t){return gn(t[ut])}function Un(t,n){return n==null?null:t[n]}function Fh(t){t[Gr]=0}function Ph(t){t[W]&1024||(t[W]|=1024,Yl(t)&&Zr(t))}function P_(t,n){for(;t>0;)n=n[qi],t--;return n}function ga(t){return!!(t[W]&9216||t[sn]?.dirty)}function Zl(t){t[Bn].changeDetectionScheduler?.notify(8),t[W]&64&&(t[W]|=1024),ga(t)&&Zr(t)}function Zr(t){t[Bn].changeDetectionScheduler?.notify(0);let n=vi(t);for(;n!==null&&!(n[W]&8192||(n[W]|=8192,!Yl(n)));)n=vi(n)}function Kl(t,n){if(Yr(t))throw new x(911,!1);t[gi]===null&&(t[gi]=[]),t[gi].push(n)}function L_(t,n){if(t[gi]===null)return;let e=t[gi].indexOf(n);e!==-1&&t[gi].splice(e,1)}function vi(t){let n=t[ut];return gn(n)?n[ut]:n}function Lh(t){return t[qo]??=[]}function Vh(t){return t.cleanup??=[]}function V_(t,n,e,i){let r=Lh(n);r.push(e),t.firstCreatePass&&Vh(t).push(i,r.length-1)}var se={lFrame:K_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var dh=!1;function j_(){return se.lFrame.elementDepthCount}function B_(){se.lFrame.elementDepthCount++}function jh(){se.lFrame.elementDepthCount--}function Ql(){return se.bindingsEnabled}function Bh(){return se.skipHydrationRootTNode!==null}function Uh(t){return se.skipHydrationRootTNode===t}function Hh(){se.skipHydrationRootTNode=null}function Z(){return se.lFrame.lView}function Ae(){return se.lFrame.tView}function le(t){return se.lFrame.contextLView=t,t[Ct]}function de(t){return se.lFrame.contextLView=null,t}function Qe(){let t=zh();for(;t!==null&&t.type===64;)t=t.parent;return t}function zh(){return se.lFrame.currentTNode}function U_(){let t=se.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Qo(t,n){let e=se.lFrame;e.currentTNode=t,e.isParent=n}function $h(){return se.lFrame.isParent}function Gh(){se.lFrame.isParent=!1}function H_(){return se.lFrame.contextLView}function Wh(){return dh}function aa(t){let n=dh;return dh=t,n}function z_(){let t=se.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function $_(t){return se.lFrame.bindingIndex=t}function Kr(){return se.lFrame.bindingIndex++}function qh(t){let n=se.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function G_(){return se.lFrame.inI18n}function W_(t,n){let e=se.lFrame;e.bindingIndex=e.bindingRootIndex=t,Xl(n)}function q_(){return se.lFrame.currentDirectiveIndex}function Xl(t){se.lFrame.currentDirectiveIndex=t}function Y_(t){let n=se.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Jl(){return se.lFrame.currentQueryIndex}function va(t){se.lFrame.currentQueryIndex=t}function Lx(t){let n=t[L];return n.type===2?n.declTNode:n.type===1?t[Rt]:null}function Yh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Lx(o),r===null||(o=o[qi],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=se.lFrame=Z_();return i.currentTNode=n,i.lView=t,!0}function ed(t){let n=Z_(),e=t[L];se.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Z_(){let t=se.lFrame,n=t===null?null:t.child;return n===null?K_(t):n}function K_(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Q_(){let t=se.lFrame;return se.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Zh=Q_;function td(){let t=Q_();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function X_(t){return(se.lFrame.contextLView=P_(t,se.lFrame.contextLView))[Ct]}function Qi(){return se.lFrame.selectedIndex}function Xi(t){se.lFrame.selectedIndex=t}function Xo(){let t=se.lFrame;return ql(t.tView,t.selectedIndex)}function ni(){se.lFrame.currentNamespace=kh}function nd(){Vx()}function Vx(){se.lFrame.currentNamespace=null}function Kh(){return se.lFrame.currentNamespace}var J_=!0;function id(){return J_}function _a(t){J_=t}function uh(t,n=null,e=null,i){let r=Qh(t,n,e,i);return r.resolveInjectorInitializers(),r}function Qh(t,n=null,e=null,i,r=new Set){let o=[e||At,S_(t)],s;return new Hr(o,n||Go(),s||null,r)}var ae=class t{static THROW_IF_NOT_FOUND=Vr;static NULL=new zo;static create(n,e){if(Array.isArray(n))return uh({name:""},e,n,"");{let i=n.name??"";return uh({name:i},n.parent,n.providers,i)}}static \u0275prov=P({token:t,providedIn:"any",factory:()=>M(ha)});static __NG_ELEMENT_ID__=-1},U=new y(""),wt=(()=>{class t{static __NG_ELEMENT_ID__=jx;static __NG_ENV_ID__=e=>e}return t})(),Ol=class extends wt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Yr(this._lView)}onDestroy(n){let e=this._lView;return Kl(e,n),()=>L_(e,n)}};function jx(){return new Ol(Z())}var ey=!1,ty=new y(""),Di=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new at(!1);debugTaskTracker=u(ty,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new re(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=P({token:t,providedIn:"root",factory:()=>new t})}return t})(),fh=class extends I{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,A_()&&(this.destroyRef=u(wt,{optional:!0})??void 0,this.pendingTasks=u(Di,{optional:!0})??void 0)}emit(n){let e=G(null);try{super.next(n)}finally{G(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof fe&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Y=fh;function Fl(...t){}function Xh(t){let n,e;function i(){t=Fl;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch(r){}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function ny(t){return queueMicrotask(()=>t()),()=>{t=Fl}}var Jh="isAngularZone",ca=Jh+"_ID",Bx=0,O=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Y(!1);onMicrotaskEmpty=new Y(!1);onStable=new Y(!1);onError=new Y(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=ey}=n;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,zx(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Jh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new x(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,Ux,Fl,Fl);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Ux={};function ep(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function Hx(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Xh(()=>{t.callbackScheduled=!1,hh(t),t.isCheckStableRunning=!0,ep(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),hh(t)}function zx(t){let n=()=>{Hx(t)},e=Bx++;t._inner=t._inner.fork({name:"angular",properties:{[Jh]:!0,[ca]:e,[ca+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if($x(c))return i.invokeTask(o,s,a,c);try{return h_(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),p_(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return h_(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!Gx(c)&&n(),p_(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,hh(t),ep(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function hh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function h_(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function p_(t){t._nesting--,ep(t)}var la=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Y;onMicrotaskEmpty=new Y;onStable=new Y;onError=new Y;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function $x(t){return iy(t,"__ignore_ng_zone__")}function Gx(t){return iy(t,"__scheduler_tick__")}function iy(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var qt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},an=new y("",{factory:()=>{let t=u(O),n=u(xe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(qt),e.handleError(i))})}}}),ry={provide:$r,useValue:()=>{let t=u(qt,{optional:!0})},multi:!0},Wx=new y("",{factory:()=>{let t=u(U).defaultView;if(!t)return;let n=u(an),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(wt).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function tp(){return yi([x_(()=>{u(Wx)})])}function ee(t,n){let[e,i,r]=Vf(t,n?.equal),o=e,s=o[qe];return o.set=i,o.update=r,o.asReadonly=rd.bind(o),o}function rd(){let t=this[qe];if(t.readonlyFn===void 0){let n=()=>this();n[qe]=t,t.readonlyFn=n}return t.readonlyFn}var Qr=new y("",{factory:()=>qx}),qx="ng";var od=new y(""),Xr=new y("",{providedIn:"platform",factory:()=>"unknown"}),ya=new y(""),Jr=new y("",{factory:()=>u(U).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Jo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Yx}return t})();function Yx(){return new Jo(Z(),Qe())}var Jn=class{},ba=new y("",{factory:()=>!0});var np=new y(""),sd=(()=>{class t{static \u0275prov=P({token:t,providedIn:"root",factory:()=>new ph})}return t})(),ph=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Pl=class{[qe];constructor(n){this[qe]=n}destroy(){this[qe].destroy()}};function Hn(t,n){let e=n?.injector??u(ae),i=n?.manualCleanup!==!0?e.get(wt):null,r,o=e.get(Jo,null,{optional:!0}),s=e.get(Jn);return o!==null?(r=Qx(o.view,s,t),i instanceof Ol&&i._lView===o.view&&(i=null)):r=Xx(t,e.get(sd),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Pl(r)}var oy=z(C({},jf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=aa(!1);try{Bf(this)}finally{aa(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=G(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],G(t)}}}),Zx=z(C({},oy),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Hi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),Kx=z(C({},oy),{consumerMarkedDirty(){this.view[W]|=8192,Zr(this.view),this.notifier.notify(13)},destroy(){if(Hi(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[zi]?.delete(this)}});function Qx(t,n,e){let i=Object.create(Kx);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=sy(i,e),t[zi]??=new Set,t[zi].add(i),i.consumerMarkedDirty(i),i}function Xx(t,n,e){let i=Object.create(Zx);return i.fn=sy(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function sy(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function bn(t){return typeof t=="function"&&t[qe]!==void 0}function ad(t){return bn(t)&&typeof t.set=="function"}var Ca=(()=>{class t{internalPendingTasks=u(Di);scheduler=u(Jn);errorHandler=u(an);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=P({token:t,providedIn:"root",factory:()=>new t})}return t})();function Aa(t){return{toString:t}.toString()}var ve=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(ve||{}),gd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Gy(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Wy=null,nt=(()=>{Wy=ay;let t=()=>ay;return t.ngInherit=!0,t})();function cS(){return Wy}function ay(t){return t.type.prototype.ngOnChanges&&(t.setInput=dS),lS}function lS(){let t=qy(this),n=t?.current;if(n){let e=t.previous;if(e===Wi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function dS(t,n,e,i,r){let o=this.declaredInputs[i],s=qy(t)||uS(t,{previous:Wi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new gd(l&&l.currentValue,e,c===Wi),Gy(t,n,r,e)}var fp="__ngSimpleChanges__";function qy(t){return Object.hasOwn(t,fp)&&t[fp]||null}function uS(t,n){return t[fp]=n}var cy=[];var we=function(t,n=null,e){for(let i=0;i<cy.length;i++){let r=cy[i];r(t,n,e)}};function fS(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=cS()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Yy(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function fd(t,n,e){Zy(t,n,3,e)}function hd(t,n,e,i){(t[W]&3)===e&&Zy(t,n,e,i)}function ip(t,n){let e=t[W];(e&3)===n&&(e&=16383,e+=1,t[W]=e)}function Zy(t,n,e,i){let r=i!==void 0?t[Gr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[Gr]+=65536),(a<o||o==-1)&&(hS(t,e,n,c),t[Gr]=(t[Gr]&4294901760)+c+2),c++}function ly(t,n){we(ve.LifecycleHookStart,t,n);let e=G(null);try{n.call(t)}finally{G(e),we(ve.LifecycleHookEnd,t,n)}}function hS(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[W]>>14<t[Gr]>>16&&(t[W]&3)===n&&(t[W]+=16384,ly(a,o)):ly(a,o)}var ts=-1,to=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function pS(t){return(t.flags&8)!==0}function mS(t){return(t.flags&16)!==0}function gS(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];vS(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Ky(t){return t===3||t===4||t===6}function vS(t){return t.charCodeAt(0)===64}function ns(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?dy(t,e,r,null,n[++i]):dy(t,e,r,null,null))}}return t}function dy(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Qy(t){return t!==ts}function vd(t){return t&32767}function _S(t){return t>>16}function _d(t,n){let e=_S(t),i=n;for(;e>0;)i=i[qi],e--;return i}var hp=!0;function uy(t){let n=hp;return hp=t,n}var yS=256,Xy=yS-1,Jy=5,bS=0,ii={};function CS(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(zr)&&(i=e[zr]),i==null&&(i=e[zr]=bS++);let r=i&Xy,o=1<<r;n.data[t+(r>>Jy)]|=o}function yd(t,n){let e=eb(t,n);if(e!==-1)return e;let i=n[L];i.firstCreatePass&&(t.injectorIndex=n.length,rp(i.data,t),rp(n,null),rp(i.blueprint,null));let r=Hp(t,n),o=t.injectorIndex;if(Qy(r)){let s=vd(r),a=_d(r,n),c=a[L].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function rp(t,n){t.push(0,0,0,0,0,0,0,0,n)}function eb(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Hp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=ob(r),i===null)return ts;if(e++,r=r[qi],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return ts}function pp(t,n,e){CS(t,n,e)}function ES(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Ky(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function tb(t,n,e){if(e&8||t!==void 0)return t;Hl(n,"NodeInjector")}function nb(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[bi],o=on(void 0);try{return r?r.get(n,i,e&8):Dh(n,i,e&8)}finally{on(o)}}return tb(i,n,e)}function ib(t,n,e,i=0,r){if(t!==null){if(n[W]&2048&&!(i&2)){let s=SS(t,n,e,i,ii);if(s!==ii)return s}let o=rb(t,n,e,i,ii);if(o!==ii)return o}return nb(n,e,i,r)}function rb(t,n,e,i,r){let o=DS(e);if(typeof o=="function"){if(!Yh(n,t,i))return i&1?tb(r,e,i):nb(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Hl(e);else return s}finally{Zh()}}else if(typeof o=="number"){let s=null,a=eb(t,n),c=ts,l=i&1?n[Jt][Rt]:null;for((a===-1||i&4)&&(c=a===-1?Hp(t,n):n[a+8],c===ts||!hy(i,!1)?a=-1:(s=n[L],a=vd(c),n=_d(c,n)));a!==-1;){let d=n[L];if(fy(o,a,d.data)){let f=wS(a,n,e,s,i,l);if(f!==ii)return f}c=n[a+8],c!==ts&&hy(i,n[L].data[a+8]===l)&&fy(o,a,n)?(s=d,a=vd(c),n=_d(c,n)):a=-1}}return r}function wS(t,n,e,i,r,o){let s=n[L],a=s.data[t+8],c=i==null?wi(a)&&hp:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=pd(a,s,e,c,l);return d!==null?xa(n,s,d,a,r):ii}function pd(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,f=i?a:a+d,m=r?a+d:l;for(let g=f;g<m;g++){let b=s[g];if(g<c&&e===b||g>=c&&b.type===e)return g}if(r){let g=s[c];if(g&&ti(g)&&g.type===e)return c}return null}function xa(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof to){let a=o;if(a.resolving)throw wh("");let c=uy(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,f=a.injectImpl?on(a.injectImpl):null,m=Yh(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&fS(e,s[e],n)}finally{f!==null&&on(f),uy(c),a.resolving=!1,Zh()}}return o}function DS(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(zr)?t[zr]:void 0;return typeof n=="number"?n>=0?n&Xy:xS:n}function fy(t,n,e){let i=1<<t;return!!(e[n+(t>>Jy)]&i)}function hy(t,n){return!(t&2)&&!(t&1&&n)}var Ji=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return ib(this._tNode,this._lView,n,jr(i),e)}};function xS(){return new Ji(Qe(),Z())}function qn(t){return Aa(()=>{let n=t.prototype.constructor,e=n[sa]||mp(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[sa]||mp(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function mp(t){return mh(t)?()=>{let n=mp(bt(t));return n&&n()}:Br(t)}function SS(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[W]&2048&&!Ko(s);){let a=rb(o,s,e,i|2,ii);if(a!==ii)return a;let c=o.parent;if(!c){let l=s[Rh];if(l){let d=l.get(e,ii,i&-5);if(d!==ii)return d}c=ob(s),s=s[qi]}o=c}return r}function ob(t){let n=t[L],e=n.type;return e===2?n.declTNode:e===1?t[Rt]:null}function Ra(t){return ES(Qe(),t)}function T(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function IS(){return ss(Qe(),Z())}function ss(t,n){return new j(_n(t,n))}var j=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=IS}return t})();function sb(t){return t instanceof j?t.nativeElement:t}function MS(){return this._results[Symbol.iterator]()}var Gn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new I}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=E_(n);(this._changesDetected=!C_(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=MS};function ab(t){return(t.flags&128)===128}var zp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(zp||{}),cb=new Map,TS=0;function AS(){return TS++}function RS(t){cb.set(t[Ci],t)}function gp(t){cb.delete(t[Ci])}var py="__ngContext__";function is(t,n){Ei(n)?(t[py]=n[Ci],RS(n)):t[py]=n}function lb(t){return ub(t[Yo])}function db(t){return ub(t[mn])}function ub(t){for(;t!==null&&!gn(t);)t=t[mn];return t}var vp;function $p(t){vp=t}function fb(){if(vp!==void 0)return vp;if(typeof document<"u")return document;throw new x(210,!1)}var hb=!1,pb=new y("",{factory:()=>hb});var my=new WeakMap;function NS(t,n){if(t==null||typeof t!="object")return;let e=my.get(t);e||(e=new WeakSet,my.set(t,e)),e.add(n)}var kS=(t,n,e,i)=>{};function OS(t,n,e,i){kS(t,n,e,i)}function Nd(t){return(t.flags&32)===32}var FS=()=>null;function mb(t,n,e=!1){return FS(t,n,e)}function gb(t,n){let e=t.contentQueries;if(e!==null){let i=G(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];va(o),a.contentQueries(2,n[s],s)}}}finally{G(i)}}}function _p(t,n,e){va(0);let i=G(null);try{n(t,e)}finally{G(i)}}function Gp(t,n,e){if(Nh(n)){let i=G(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{G(i)}}}var Wn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Wn||{});var cd;function PS(){if(cd===void 0&&(cd=null,Gi.trustedTypes))try{cd=Gi.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return cd}function kd(t){return PS()?.createHTML(t)||t}var ld;function LS(){if(ld===void 0&&(ld=null,Gi.trustedTypes))try{ld=Gi.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return ld}function gy(t){return LS()?.createScriptURL(t)||t}var xi=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Ll})`}},yp=class extends xi{getTypeName(){return"HTML"}},bp=class extends xi{getTypeName(){return"Style"}},Cp=class extends xi{getTypeName(){return"Script"}},Ep=class extends xi{getTypeName(){return"URL"}},wp=class extends xi{getTypeName(){return"ResourceURL"}};function Yn(t){return t instanceof xi?t.changingThisBreaksApplicationSecurity:t}function Ii(t,n){let e=vb(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Ll})`)}return e===n}function vb(t){return t instanceof xi&&t.getTypeName()||null}function Wp(t){return new yp(t)}function qp(t){return new bp(t)}function Yp(t){return new Cp(t)}function Zp(t){return new Ep(t)}function Kp(t){return new wp(t)}function VS(t){let n=new xp(t);return jS()?new Dp(n):n}var Dp=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(kd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch(e){return null}}},xp=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=kd(n),e}};function jS(){try{return!!new window.DOMParser().parseFromString(kd(""),"text/html")}catch(t){return!1}}var BS=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Na(t){return t=String(t),t.match(BS)?t:"unsafe:"+t}function Mi(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function ka(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var _b=Mi("area,br,col,hr,img,wbr"),yb=Mi("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),bb=Mi("rp,rt"),US=ka(bb,yb),HS=ka(yb,Mi("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),zS=ka(bb,Mi("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),vy=ka(_b,HS,zS,US),Cb=Mi("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),$S=Mi("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),GS=Mi("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),WS=ka(Cb,$S,GS),qS=Mi("script,style,template"),Sp=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=KS(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=ZS(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=_y(n).toLowerCase();if(!vy.hasOwnProperty(e))return this.sanitizedSomething=!0,!qS.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!WS.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;Cb[a]&&(c=Na(c)),this.buf.push(" ",s,'="',yy(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=_y(n).toLowerCase();vy.hasOwnProperty(e)&&!_b.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(yy(n))}};function YS(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function ZS(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw Eb(n);return n}function KS(t){let n=t.firstChild;if(n&&YS(t,n))throw Eb(n);return n}function _y(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function Eb(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var QS=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,XS=/([^\#-~ |!])/g;function yy(t){return t.replace(/&/g,"&amp;").replace(QS,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(XS,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var dd;function Qp(t,n){let e=null;try{dd=dd||VS(t);let i=n?String(n):"";e=dd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=dd.getInertBodyElement(i)}while(i!==o);let a=new Sp().sanitizeChildren(by(e)||e);return kd(a)}finally{if(e){let i=by(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function by(t){return"content"in t&&JS(t)?t.content:null}function JS(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var eI=/^>|^->|<!--|-->|--!>|<!-$/g,tI=/(<|>)/g,nI="\u200B$1\u200B";function iI(t){return t.replace(eI,n=>n.replace(tI,nI))}function rI(t,n){return t.createText(n)}function oI(t,n,e){t.setValue(n,e)}function sI(t,n){return t.createComment(iI(n))}function wb(t,n,e){return t.createElement(n,e)}function bd(t,n,e,i,r){t.insertBefore(n,e,i,r)}function Db(t,n,e){t.appendChild(n,e)}function Cy(t,n,e,i,r){i!==null?bd(t,n,e,i,r):Db(t,n,e)}function aI(t,n,e,i){t.removeChild(null,n,e,i)}function cI(t,n,e){t.setAttribute(n,"style",e)}function lI(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function xb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&gS(t,n,i),r!==null&&lI(t,n,r),o!==null&&cI(t,n,o)}var ct=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(ct||{});function Sb(t){let n=Mb();return n?n.sanitize(ct.URL,t)||"":Ii(t,"URL")?Yn(t):Na(Ul(t))}function Ib(t){let n=Mb();if(n)return gy(n.sanitize(ct.RESOURCE_URL,t)||"");if(Ii(t,"ResourceURL"))return gy(Yn(t));throw new x(904,!1)}var dI={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function uI(t,n){return dI[t.toLowerCase()]?.[n.toLowerCase()]===!0?Ib:Sb}function Xp(t,n,e){return uI(n,e)(t)}function Mb(){let t=Z();return t&&t[Bn].sanitizer}function fI(t){return t instanceof Function?t():t}function hI(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var Tb="ng-template";function pI(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&hI(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Jp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Jp(t){return t.type===4&&t.value!==Tb}function mI(t,n,e){let i=t.type===4&&!e?Tb:t.value;return n===i}function gI(t,n,e){let i=4,r=t.attrs,o=r!==null?yI(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!zn(i)&&!zn(c))return!1;if(s&&zn(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!mI(t,c,e)||c===""&&n.length===1){if(zn(i))return!1;s=!0}}else if(i&8){if(r===null||!pI(t,r,c,e)){if(zn(i))return!1;s=!0}}else{let l=n[++a],d=vI(c,r,Jp(t),e);if(d===-1){if(zn(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(zn(i))return!1;s=!0}}}}return zn(i)||s}function zn(t){return(t&1)===0}function vI(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return bI(n,t)}function Ab(t,n,e=!1){for(let i=0;i<n.length;i++)if(gI(t,n[i],e))return!0;return!1}function _I(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function yI(t){for(let n=0;n<t.length;n++){let e=t[n];if(Ky(e))return n}return t.length}function bI(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function CI(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function Ey(t,n){return t?":not("+n.trim()+")":n}function EI(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!zn(s)&&(n+=Ey(o,r),r=""),i=s,o=o||!zn(i);e++}return r!==""&&(n+=Ey(o,r)),n}function wI(t){return t.map(EI).join(",")}function DI(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!zn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Cn={},ri=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(ri||{}),xI;function em(t,n){return xI(t,n)}var Cz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Ip=new WeakMap;function Rb(t){return t?t[qi]??t:null}var Ea=new WeakSet;function SI(t,n,e){let i=Ip.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=Rb(e);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),Ea.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function II(t,n,e){let i=Rb(e),r=Ip.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Ip.set(t,[{el:n,declarationView:i}])}var no=new Set,Od=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Od||{}),oi=new y(""),wy=new Set;function tr(t){wy.has(t)||(wy.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Fd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=P({token:t,providedIn:"root",factory:()=>new t})}return t})(),tm=[0,1,2,3],nm=(()=>{class t{ngZone=u(O);scheduler=u(Jn);errorHandler=u(qt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(oi,{optional:!0})}execute(){let e=this.sequences.size>0;e&&we(ve.AfterRenderHooksStart),this.executing=!0;for(let i of tm)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&we(ve.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Wr]??=[]).push(e),Zr(i),i[W]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Od.AFTER_NEXT_RENDER,e):e()}static \u0275prov=P({token:t,providedIn:"root",factory:()=>new t})}return t})(),Sa=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Wr];n&&(this.view[Wr]=n.filter(e=>e!==this))}};function En(t,n){let e=n?.injector??u(ae);return tr("NgAfterNextRender"),TI(t,e,n,!0)}function MI(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function TI(t,n,e,i){let r=n.get(Fd);r.impl??=n.get(nm);let o=n.get(oi,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(wt):null,a=n.get(Jo,null,{optional:!0}),c=new Sa(r.impl,MI(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var Nb=new y("",{factory:()=>{let t=u(xe),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function kb(t,n,e){let i=t.get(Nb);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function AI(t,n){let e=t.get(Nb);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function RI(t,n){for(let[e,i]of n)kb(t,i.animateFns)}function Dy(t,n,e,i){let r=t?.[Zi]?.enter;n!==null&&r&&r.has(e.index)&&RI(i,r)}function xy(t,n,e,i){try{e.get(ha)}catch(s){return i(!1)}let r=t?.[Zi];r?.enter?.has(n.index)&&AI(e,r.enter.get(n.index).animateFns);let o=NI(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];Pd(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&no.add(t[Ci]),kb(e,()=>kI(t,n,r||void 0,o,i),r||void 0)}function NI(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let c=t[L].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function kI(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&Pd(t,n,o),o.length>0){let s=e||t?.[Zi];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),FI(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&no.delete(t[Ci]),r(!0)})}else t&&no.delete(t[Ci]),r(!1)}function Pd(t,n,e){if(n.type&12){let r=t[n.index];if(gn(r))for(let o=Et;o<r.length;o++){let s=r[o];s[L].type===2&&OI(s,e)}}let i=n.child;for(;i;)Pd(t,i,e),i=i.next}function OI(t,n){let e=t[Zi];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[L].firstChild;for(;i;)Pd(t,i,n),i=i.next}function FI(t,n,e){n.then(()=>{t[Zi]?.running===n&&(t[Zi].running=void 0,no.delete(t[Ci])),e(!0)})}function es(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;gn(r)?c=r:Ei(r)&&(l=!0,r=r[jn]);let d=vn(r);t===0&&i!==null?(Dy(a,i,o,e),s==null?Db(n,i,d):bd(n,i,d,s||null,!0)):t===1&&i!==null?(Dy(a,i,o,e),bd(n,i,d,s||null,!0),SI(o,d,a)):t===2?(a?.[Zi]?.leave?.has(o.index)&&II(o,d,a),Ea.delete(d),xy(a,o,e,f=>{if(Ea.has(d)){Ea.delete(d);return}aI(n,d,l,f)})):t===3&&(Ea.delete(d),xy(a,o,e,()=>{n.destroyNode(d)})),c!=null&&GI(n,t,e,c,o,i,s)}}function PI(t,n){Ob(t,n),n[jn]=null,n[Rt]=null}function LI(t,n,e,i,r,o){i[jn]=r,i[Rt]=n,Ld(t,i,e,1,r,o)}function Ob(t,n){n[Bn].changeDetectionScheduler?.notify(9),Ld(t,n,n[Me],2,null,null)}function VI(t){let n=t[Yo];if(!n)return op(t[L],t);for(;n;){let e=null;if(Ei(n))e=n[Yo];else{let i=n[Et];i&&(e=i)}if(!e){for(;n&&!n[mn]&&n!==t;)Ei(n)&&op(n[L],n),n=n[ut];n===null&&(n=t),Ei(n)&&op(n[L],n),e=n&&n[mn]}n=e}}function im(t,n){let e=t[qr],i=e.indexOf(n);e.splice(i,1)}function rm(t,n){if(Yr(n))return;let e=n[Me];e.destroyNode&&Ld(t,n,e,3,null,null),VI(n)}function op(t,n){if(Yr(n))return;let e=G(null);try{n[W]&=-129,n[W]|=256,n[sn]&&Hi(n[sn]),BI(t,n),jI(t,n),n[L].type===1&&n[Me].destroy();let i=n[Yi];if(i!==null&&gn(n[ut])){i!==n[ut]&&im(i,n);let r=n[ei];r!==null&&r.detachView(t)}gp(n)}finally{G(e)}}function jI(t,n){let e=t.cleanup,i=n[qo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[qo]=null);let r=n[gi];if(r!==null){n[gi]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[zi];if(o!==null){n[zi]=null;for(let s of o)s.destroy()}}function BI(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof to)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];we(ve.LifecycleHookStart,a,c);try{c.call(a)}finally{we(ve.LifecycleHookEnd,a,c)}}else{we(ve.LifecycleHookStart,r,o);try{o.call(r)}finally{we(ve.LifecycleHookEnd,r,o)}}}}}function Fb(t,n,e){return UI(t,n.parent,e)}function UI(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[jn];if(wi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Wn.None||r===Wn.Emulated)return null}return _n(i,e)}function Pb(t,n,e){return zI(t,n,e)}function HI(t,n,e){return t.type&40?_n(t,e):null}var zI=HI,Sy;function om(t,n,e,i){let r=Fb(t,i,n),o=n[Me],s=i.parent||n[Rt],a=Pb(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)Cy(o,r,e[c],a,!1);else Cy(o,r,e,a,!1);Sy!==void 0&&Sy(o,i,n,e,r)}function wa(t,n){if(n!==null){let e=n.type;if(e&3)return _n(n,t);if(e&4)return Mp(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return wa(t,i);{let r=t[n.index];return gn(r)?Mp(-1,r):vn(r)}}else{if(e&128)return wa(t,n.next);if(e&32)return em(n,t)()||vn(t[n.index]);{let i=Lb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=vi(t[Jt]);return wa(r,i)}else return wa(t,n.next)}}}return null}function Lb(t,n){if(n!==null){let i=t[Jt][Rt],r=n.projection;return i.projection[r]}return null}function Mp(t,n){let e=Et+t+1;if(e<n.length){let i=n[e],r=i[L].firstChild;if(r!==null)return wa(i,r)}return n[Ki]}function sm(t,n,e,i,r,o,s){for(;e!=null;){let a=i[bi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&is(vn(c),i),e.flags|=2),!Nd(e))if(l&8)sm(t,n,e.child,i,r,o,!1),es(n,t,a,r,c,e,o,i);else if(l&32){let d=em(e,i),f;for(;f=d();)es(n,t,a,r,f,e,o,i);es(n,t,a,r,c,e,o,i)}else l&16?Vb(t,n,i,e,r,o):es(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function Ld(t,n,e,i,r,o){sm(e,i,t.firstChild,n,r,o,!1)}function $I(t,n,e){let i=n[Me],r=Fb(t,e,n),o=e.parent||n[Rt],s=Pb(o,e,n);Vb(i,0,n,e,r,s)}function Vb(t,n,e,i,r,o){let s=e[Jt],c=s[Rt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];es(n,t,e[bi],r,d,i,o,e)}else{let l=c,d=s[ut];ab(i)&&(l.flags|=128),sm(t,n,l,d,r,o,!0)}}function GI(t,n,e,i,r,o,s){let a=i[Ki],c=vn(i);a!==c&&es(n,t,e,o,a,r,s);for(let l=Et;l<i.length;l++){let d=i[l];Ld(d[L],d,t,n,o,a)}}function WI(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:ri.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=ri.Important),t.setStyle(e,i,r,o))}}function am(t,n,e,i,r,o,s,a,c,l,d){let f=Ke+i,m=f+r,g=qI(f,m),b=typeof l=="function"?l():l;return g[L]={type:t,blueprint:g,template:e,queries:null,viewQuery:a,declTNode:n,data:g.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:b,incompleteFirstPass:!1,ssrId:d}}function qI(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Cn);return e}function YI(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=am(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function cm(t,n,e,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[jn]=r,f[W]=i|4|128|8|64|1024,(l!==null||t&&t[W]&2048)&&(f[W]|=2048),Fh(f),f[ut]=f[qi]=t,f[Ct]=e,f[Bn]=s||t&&t[Bn],f[Me]=a||t&&t[Me],f[bi]=c||t&&t[bi]||null,f[Rt]=o,f[Ci]=AS(),f[Wo]=d,f[Rh]=l,f[Jt]=n.type==2?t[Jt]:f,f}function ZI(t,n,e){let i=_n(n,t),r=YI(e),o=t[Bn].rendererFactory,s=lm(t,cm(t,r,null,jb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function jb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Bb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function lm(t,n){return t[Yo]?t[Ah][mn]=n:t[Yo]=n,t[Ah]=n,n}function _(t=1){Ub(Ae(),Z(),Qi()+t,!1)}function Ub(t,n,e,i){if(!i)if((n[W]&3)===3){let o=t.preOrderCheckHooks;o!==null&&fd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&hd(n,o,0,e)}Xi(e)}var Vd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Vd||{});function io(t,n,e,i){let r=G(null);try{let[o,s,a]=t.inputs[e],c=null;(s&Vd.SignalBased)!==0&&(c=n[o][qe]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):Gy(n,c,o,i)}finally{G(r)}}function Hb(t,n,e,i,r){let o=Qi(),s=i&2;try{Xi(-1),s&&n.length>Ke&&Ub(t,n,Ke,!1);let a=s?ve.TemplateUpdateStart:ve.TemplateCreateStart;we(a,r,e),e(i,r)}finally{Xi(o);let a=s?ve.TemplateUpdateEnd:ve.TemplateCreateEnd;we(a,r,e)}}function jd(t,n,e){tM(t,n,e),(e.flags&64)===64&&nM(t,n,e)}function Oa(t,n,e=_n){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function KI(t,n,e,i){let o=i.get(pb,hb)||e===Wn.ShadowDom||e===Wn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return QI(s),s}function QI(t){XI(t)}var XI=()=>null;function JI(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function zb(t,n,e,i,r,o){let s=n[L];if(hm(t,s,n,e,i)){wi(t)&&eM(n,t.index);return}t.type&3&&(e=JI(e)),$b(t,n,e,i,r,o)}function $b(t,n,e,i,r,o){if(t.type&3){let s=_n(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function eM(t,n){let e=yn(n,t);e[W]&16||(e[W]|=64)}function tM(t,n,e){let i=e.directiveStart,r=e.directiveEnd;wi(e)&&ZI(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||yd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=xa(n,t,s,e);if(is(c,n),o!==null&&sM(n,s-i,c,a,e,o),ti(a)){let l=yn(e.index,n);l[Ct]=xa(n,t,s,e)}}}function nM(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=q_();try{Xi(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Xl(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&iM(c,l)}}finally{Xi(-1),Xl(s)}}function iM(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function dm(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Ab(n,o.selectors,!1)&&(i??=[],ti(o)?i.unshift(o):i.push(o))}return i}function rM(t,n,e,i,r,o){let s=_n(t,n);oM(n[Me],s,o,t.value,e,i,r)}function oM(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Ul(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function sM(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];io(i,e,c,l)}}function um(t,n,e,i,r){let o=Ke+e,s=n[L],a=r(s,n,t,i,e);n[o]=a,Qo(t,!0);let c=t.type===2;return c?(xb(n[Me],a,t),(j_()===0||Zo(t))&&is(a,n),B_()):is(a,n),id()&&(!c||!Nd(t))&&om(s,n,a,t),t}function fm(t){let n=t;return $h()?Gh():(n=n.parent,Qo(n,!1)),n}function aM(t,n){let e=t[bi];if(!e)return;let i;try{i=e.get(an,null)}catch(r){i=null}i?.(n)}function hm(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];io(f,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];io(d,l,i,r),a=!0}return a}function cM(t,n,e,i,r,o){let s=null,a=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let m=0;m<f.length;m+=2){let g=f[m];if(g>=a&&g<=c){let b=n.data[g],S=f[m+1];io(b,e[g],S,o),l=!0}else if(g>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(io(i,e[s],r,o),l=!0),l}function lM(t,n){let e=yn(n,t),i=e[L];dM(i,e);let r=e[jn];r!==null&&e[Wo]===null&&(e[Wo]=mb(r,e[bi])),we(ve.ComponentStart);try{pm(i,e,e[Ct])}finally{we(ve.ComponentEnd,e[Ct])}}function dM(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function pm(t,n,e){ed(n);try{let i=t.viewQuery;i!==null&&_p(1,i,e);let r=t.template;r!==null&&Hb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[ei]?.finishViewCreation(t),t.staticContentQueries&&gb(t,n),t.staticViewQueries&&_p(2,t.viewQuery,e);let o=t.components;o!==null&&uM(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[W]&=-5,td()}}function uM(t,n){for(let e=0;e<n.length;e++)lM(t,n[e])}function mm(t,n,e,i){let r=G(null);try{let o=n.tView,a=t[W]&4096?4096:16,c=cm(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[Yi]=l;let d=t[ei];return d!==null&&(c[ei]=d.createEmbeddedView(o)),pm(o,c,e),c}finally{G(r)}}function Cd(t,n){return!n||n.firstChild===null||ab(t)}function Ia(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(vn(o)),gn(o)&&Gb(o,i);let s=e.type;if(s&8)Ia(t,n,e.child,i);else if(s&32){let a=em(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=Lb(n,e);if(Array.isArray(a))i.push(...a);else{let c=vi(n[Jt]);Ia(c[L],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Gb(t,n){for(let e=Et;e<t.length;e++){let i=t[e],r=i[L].firstChild;r!==null&&Ia(i[L],i,r,n)}t[Ki]!==t[jn]&&n.push(t[Ki])}function Wb(t){if(t[Wr]!==null){for(let n of t[Wr])n.impl.addSequence(n);t[Wr].length=0}}var qb=[];function fM(t){return t[sn]??hM(t)}function hM(t){let n=qb.pop()??Object.create(mM);return n.lView=t,n}function pM(t){t.lView[sn]!==t&&(t.lView=null,qb.push(t))}var mM=z(C({},ji),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Zr(t.lView)},consumerOnSignalRead(){this.lView[sn]=this}});function gM(t){let n=t[sn]??Object.create(vM);return n.lView=t,n}var vM=z(C({},ji),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=vi(t.lView);for(;n&&!Yb(n[L]);)n=vi(n);n&&Ph(n)},consumerOnSignalRead(){this.lView[sn]=this}});function Yb(t){return t.type!==2}function Zb(t){if(t[zi]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[zi])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[W]&8192)}}var _M=100;function Kb(t,n=0){let i=t[Bn].rendererFactory,r=!1;r||i.begin?.();try{yM(t,n)}finally{r||i.end?.()}}function yM(t,n){let e=Wh();try{aa(!0),Tp(t,n);let i=0;for(;ga(t);){if(i===_M)throw new x(103,!1);i++,Tp(t,1)}}finally{aa(e)}}function bM(t,n,e,i){if(Yr(n))return;let r=n[W],o=!1,s=!1;ed(n);let a=!0,c=null,l=null;o||(Yb(t)?(l=fM(n),c=hi(l)):Kc()===null?(a=!1,l=gM(n),c=hi(l)):n[sn]&&(Hi(n[sn]),n[sn]=null));try{Fh(n),$_(t.bindingStartIndex),e!==null&&Hb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let g=t.preOrderCheckHooks;g!==null&&fd(n,g,null)}else{let g=t.preOrderHooks;g!==null&&hd(n,g,0,null),ip(n,0)}if(s||CM(n),Zb(n),Qb(n,0),t.contentQueries!==null&&gb(t,n),!o)if(d){let g=t.contentCheckHooks;g!==null&&fd(n,g)}else{let g=t.contentHooks;g!==null&&hd(n,g,1),ip(n,1)}wM(t,n);let f=t.components;f!==null&&Jb(n,f,0);let m=t.viewQuery;if(m!==null&&_p(2,m,i),!o)if(d){let g=t.viewCheckHooks;g!==null&&fd(n,g)}else{let g=t.viewHooks;g!==null&&hd(n,g,2),ip(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Wl]){for(let g of n[Wl])g();n[Wl]=null}o||(Wb(n),n[W]&=-73)}catch(d){throw o||Zr(n),d}finally{l!==null&&(Ui(l,c),a&&pM(l)),td()}}function Qb(t,n){for(let e=lb(t);e!==null;e=db(e))for(let i=Et;i<e.length;i++){let r=e[i];Xb(r,n)}}function CM(t){for(let n=lb(t);n!==null;n=db(n)){if(!(n[W]&2))continue;let e=n[qr];for(let i=0;i<e.length;i++){let r=e[i];Ph(r)}}}function EM(t,n,e){we(ve.ComponentStart);let i=yn(n,t);try{Xb(i,e)}finally{we(ve.ComponentEnd,i[Ct])}}function Xb(t,n){Yl(t)&&Tp(t,n)}function Tp(t,n){let i=t[L],r=t[W],o=t[sn],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&No(o)),s||=!1,o&&(o.dirty=!1),t[W]&=-9217,s)bM(i,t,i.template,t[Ct]);else if(r&8192){let a=G(null);try{Zb(t),Qb(t,1);let c=i.components;c!==null&&Jb(t,c,1),Wb(t)}finally{G(a)}}}function Jb(t,n,e){for(let i=0;i<n.length;i++)EM(t,n[i],e)}function wM(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Xi(~r);else{let o=r,s=e[++i],a=e[++i];W_(s,o);let c=n[o];we(ve.HostBindingsUpdateStart,c);try{a(2,c)}finally{we(ve.HostBindingsUpdateEnd,c)}}}}finally{Xi(-1)}}function gm(t,n){let e=Wh()?64:1088;for(t[Bn].changeDetectionScheduler?.notify(n);t;){t[W]|=e;let i=vi(t);if(Ko(t)&&!i)return t;t=i}return null}function e0(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function DM(t,n){let e=Et+n;if(e<t.length)return t[e]}function vm(t,n,e,i=!0){let r=n[L];if(SM(r,n,t,e),i){let s=Mp(e,t),a=n[Me],c=a.parentNode(t[Ki]);c!==null&&LI(r,t[Rt],a,n,c,s)}let o=n[Wo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function xM(t,n){let e=Ed(t,n);return e!==void 0&&rm(e[L],e),e}function Ed(t,n){if(t.length<=Et)return;let e=Et+n,i=t[e];if(i){let r=i[Yi];r!==null&&r!==t&&im(r,i),n>0&&(t[e-1][mn]=i[mn]);let o=fa(t,Et+n);PI(i[L],i);let s=o[ei];s!==null&&s.detachView(o[L]),i[ut]=null,i[mn]=null,i[W]&=-129}return i}function SM(t,n,e,i){let r=Et+i,o=e.length;i>0&&(e[r-1][mn]=n),i<o-Et?(n[mn]=e[r],xh(e,Et+i,n)):(e.push(n),n[mn]=null),n[ut]=e;let s=n[Yi];s!==null&&e!==s&&t0(s,n);let a=n[ei];a!==null&&a.insertView(t),Zl(n),n[W]|=128}function t0(t,n){let e=t[qr],i=n[ut];if(Ei(i))t[W]|=2;else{let r=i[ut][Jt];n[Jt]!==r&&(t[W]|=2)}e===null?t[qr]=[n]:e.push(n)}var er=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[L];return Ia(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[Ct]}set context(n){this._lView[Ct]=n}get destroyed(){return Yr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[ut];if(gn(n)){let e=n[ma],i=e?e.indexOf(this):-1;i>-1&&(Ed(n,i),fa(e,i))}this._attachedToViewContainer=!1}rm(this._lView[L],this._lView)}onDestroy(n){Kl(this._lView,n)}markForCheck(){gm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[W]&=-129}reattach(){Zl(this._lView),this._lView[W]|=128}detectChanges(){this._lView[W]|=1024,Kb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ko(this._lView),e=this._lView[Yi];e!==null&&!n&&im(e,this._lView),Ob(this._lView[L],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=n;let e=Ko(this._lView),i=this._lView[Yi];i!==null&&!e&&t0(i,this._lView),Zl(this._lView)}};var en=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=IM;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=mm(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new er(o)}}return t})();function IM(){return Bd(Qe(),Z())}function Bd(t,n){return t.type&4?new en(n,t,ss(t,n)):null}function as(t,n,e,i,r){let o=t.data[n];if(o===null)o=MM(t,n,e,i,r),G_()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=U_();o.injectorIndex=s===null?-1:s.injectorIndex}return Qo(o,!0),o}function MM(t,n,e,i,r){let o=zh(),s=$h(),a=s?o:o&&o.parent,c=t.data[n]=AM(t,a,e,n,i,r);return TM(t,c,o,s),c}function TM(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function AM(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Bh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Kh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var RM=()=>null,NM=()=>null;function Ap(t,n){return RM(t,n)}function kM(t,n,e){return NM(t,n,e)}var n0=class{},tt=class{},je=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>OM()}return t})();function OM(){let t=Z(),n=Qe(),e=yn(n.index,t);return(Ei(e)?e:t)[Me]}var i0=(()=>{class t{static \u0275prov=P({token:t,providedIn:"root",factory:()=>null})}return t})();function r0(t){return t.debugInfo?.className||t.type.name||null}var md={},wd=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,md,i);return r!==md||e===md?r:this.parentInjector.get(n,e,i)}};function _m(t){return s0(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function o0(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function s0(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function FM(t,n,e){return t[n]=e}function Ti(t,n,e){if(e===Cn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function eo(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&NS(r,o);let s=wi(t)?yn(t.index,n):n;gm(s,5);let a=n[Ct],c=Iy(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=Iy(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Iy(t,n,e,i){let r=G(null);try{return we(ve.OutputStart,n,e),e(i)!==!1}catch(o){return aM(t,o),!1}finally{we(ve.OutputEnd,n,e),G(r)}}function ym(t,n,e,i,r,o,s,a){let c=Zo(t),l=!1,d=null;if(!i&&c&&(d=LM(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=_n(t,e),m=i?i(f):f;OS(e,m,o,a),i||(a.__ngNativeEl__=f);let g=r.listen(m,o,a);if(!PM(o)){let b=i?S=>i(vn(S[t.index])):t.index;a0(b,n,e,o,a,g,!1)}}return l}function PM(t){return t.startsWith("animation")||t.startsWith("transition")}function LM(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[qo],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function a0(t,n,e,i,r,o,s){let a=n.firstCreatePass?Vh(n):null,c=Lh(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function My(t,n,e,i,r){let o=null,s=null,a=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let d=t.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let m=d[f];if(m>=s&&m<=a)c=!0,Dd(t,n,m,d[f+1],i,r);else if(m>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,Dd(t,n,o,i,i,r)),c}function Dd(t,n,e,i,r,o){let s=n[e],a=n[L],l=a.data[e].outputs[i],f=s[l].subscribe(o);a0(t.index,a,n,r,o,f,!0)}function Oe(){VM()}function VM(){let t=Z(),n=Ae(),e=Qe();if(n.firstCreatePass&&BM(n,e),e.controlDirectiveIndex===-1)return;tr("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new xd(t,n,e))}function Fe(){jM()}function jM(){let t=Z(),n=Ae(),e=Xo();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new xd(t,n,e))}var xd=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return _n(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];My(this.tNode,this.lView,i,n,eo(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];My(this.tNode,this.lView,i,e,eo(this.tNode,this.lView,n))}listenToDom(n,e){ym(this.tNode,this.tView,this.lView,void 0,this.lView[Me],n,e,eo(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];io(a,c,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];io(l,d,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";cM(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=Ty(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=Ty(o.directive);s!==null&&i.push(...s)}}}return e}};function Ty(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function BM(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}UM(t,n)}function UM(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(Ay(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(Ay(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[m,g,b]=f;if(c>=g&&c<=b)return n.flags|=r,n.customControlIndex=m,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function Ay(t,n){return HM(t,n)&&zM(t,n+"Change")}function HM(t,n){return n in t.inputs}function zM(t,n){return n in t.outputs}var Rp=Symbol("BINDING");var oo=new y("");function Sd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Vl(r,a);else if(o==2){let c=a,l=n[++s];i=Vl(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function w(t,n=0){let e=Z();if(e===null)return M(t,n);let i=Qe();return ib(i,e,bt(t),n)}function Fa(){let t="invalid";throw new Error(t)}function c0(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}WM(t,n,e,a,o,c,l)}o!==null&&i!==null&&$M(e,i,o)}function $M(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new x(-301,!1);i.push(n[r],o)}}function GM(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function WM(t,n,e,i,r,o,s){let a=i.length,c=null;for(let m=0;m<a;m++){let g=i[m];c===null&&ti(g)&&(c=g,GM(t,e,m)),pp(yd(e,n),t,g.type)}XM(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let g=i[m];g.providersResolver&&g.providersResolver(g)}let l=!1,d=!1,f=Bb(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let m=0;m<a;m++){let g=i[m];if(e.mergedAttrs=ns(e.mergedAttrs,g.hostAttrs),YM(t,e,n,f,g),QM(f,g,r),s!==null&&s.has(g)){let[S,N]=s.get(g);e.directiveToIndex.set(g.type,[f,S+e.directiveStart,N+e.directiveStart])}else(o===null||!o.has(g))&&e.directiveToIndex.set(g.type,f);g.contentQueries!==null&&(e.flags|=4),(g.hostBindings!==null||g.hostAttrs!==null||g.hostVars!==0)&&(e.flags|=64);let b=g.type.prototype;!l&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}qM(t,e,o)}function qM(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))Ry(0,n,r,i),Ry(1,n,r,i),ky(n,i,!1);else{let o=e.get(r);Ny(0,n,o,i),Ny(1,n,o,i),ky(n,i,!0)}}}function Ry(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),l0(n,o)}}function Ny(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),l0(n,s)}}function l0(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function ky(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Jp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function YM(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Br(r.type,!0)),s=new to(o,ti(r),w,null);t.blueprint[i]=s,e[i]=s,ZM(t,n,i,Bb(t,e,r.hostVars,Cn),r)}function ZM(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;KM(s)!=a&&s.push(a),s.push(e,i,o)}}function KM(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function QM(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;ti(n)&&(e[""]=t)}}function XM(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function bm(t,n,e,i,r,o,s,a){let c=n[L],l=c.consts,d=Un(l,s),f=as(c,t,e,i,d);return o&&c0(c,n,f,Un(l,a),r),f.mergedAttrs=ns(f.mergedAttrs,f.attrs),f.attrs!==null&&Sd(f,f.attrs,!1),f.mergedAttrs!==null&&Sd(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Cm(t,n){Yy(t,n),Nh(n)&&t.queries.elementEnd(n)}function JM(t,n,e,i,r,o){let s=n.consts,a=Un(s,r),c=as(n,t,e,i,a);if(c.mergedAttrs=ns(c.mergedAttrs,c.attrs),o!=null){let l=Un(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Sd(c,c.attrs,!1),c.mergedAttrs!==null&&Sd(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var d0=typeof ShadowRoot<"u",eT=typeof Document<"u";function tT(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Vd.SignalBased)!==0};return r&&(o.transform=r),o})}function nT(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function iT(t,n,e){let i=n instanceof xe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new wd(e,i):e}function rT(t){let n=t.get(tt,null);if(n===null)throw new x(407,!1);let e=t.get(i0,null),i=t.get(Jn,null),r=t.get(oi,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function oT(t,n){let e=u0(t);return wb(n,e,e==="svg"?kh:e==="math"?N_:null)}function sT(t){if(t?.toLowerCase()==="script")throw new x(905,!1)}function u0(t){return(t.selectors[0][0]||"div").toLowerCase()}var rs=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=tT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=nT(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=wI(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){we(ve.DynamicComponentStart);let a=G(null);try{let c=this.componentDef,l=iT(c,r||this.ngModule,n),d=rT(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(r0(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{G(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=aT(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?KI(l,r,a.encapsulation,e):oT(a,l);sT(d?.tagName);let f=e.get(oo,null),m=cT(d,()=>e.get(U,null)??fb());f&&f.addHost(m);let g=s?.some(Oy)||o?.some(N=>typeof N!="function"&&N.bindings.some(Oy)),b=cm(null,c,null,512|jb(a),null,null,n,l,e,null,mb(d,e,!0));f&&d0&&m instanceof ShadowRoot&&Kl(b,()=>{f.removeHost(m)}),b[Ke]=d,ed(b);let S=null;try{let N=bm(Ke,b,2,"#host",()=>c.directiveRegistry,!0,0);xb(l,d,N),is(d,b),jd(c,b,N),Gp(c,N,b),Cm(c,N),i!==void 0&&dT(N,this.ngContentSelectors,i),S=yn(N.index,b),b[Ct]=S[Ct],pm(c,b,null)}catch(N){throw S!==null&&gp(S),gp(b),N}finally{we(ve.DynamicComponentEnd),td()}return new Id(this.componentType,b,!!g)}};function aT(t,n,e,i){let r=t?["ng-version","22.0.5"]:DI(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Rp].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let m of f.bindings){a+=m[Rp].requiredVars;let g=d+1;m.create&&(m.targetIdx=g,(o??=[]).push(m)),m.update&&(m.targetIdx=g,(s??=[]).push(m))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,m=Eh(f);c.push(m)}return am(0,null,lT(o,s),1,a,c,null,null,null,[r],null)}function cT(t,n){let e=t.getRootNode?.();return eT&&e instanceof Document?e.head:e&&d0&&e instanceof ShadowRoot?e:n().head}function lT(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Oy(t){let n=t[Rp].kind;return n==="input"||n==="twoWay"}var Id=class extends n0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=ql(e[L],Ke),this.location=ss(this._tNode,e),this.instance=yn(this._tNode.index,e)[Ct],this.hostView=this.changeDetectorRef=new er(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=hm(i,r[L],r,n,e);this.previousInputValues.set(n,e);let s=yn(i.index,r);gm(s,1)}get injector(){return new Ji(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function dT(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Nt=(()=>{class t{static __NG_ELEMENT_ID__=uT}return t})();function uT(){let t=Qe();return f0(t,Z())}var Np=class t extends Nt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return ss(this._hostTNode,this._hostLView)}get injector(){return new Ji(this._hostTNode,this._hostLView)}get parentInjector(){let n=Hp(this._hostTNode,this._hostLView);if(Qy(n)){let e=_d(n,this._hostLView),i=vd(n),r=e[L].data[i+8];return new Ji(r,e)}else return new Ji(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Fy(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Et}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Ap(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Cd(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new rs($i(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let ie=this.parentInjector.get(xe,null);ie&&(o=ie)}let m=$i(d.componentType??{}),g=Ap(this._lContainer,m?.id??null),b=g?.firstChild??null,S=d.create(f,r,b,o,s,a);return this.insertImpl(S.hostView,c,Cd(this._hostTNode,g)),S}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(F_(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[ut],l=new t(c,c[Rt],c[ut]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return vm(s,r,o,i),n.attachToViewContainerRef(),xh(sp(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Fy(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Ed(this._lContainer,e);i&&(fa(sp(this._lContainer),e),rm(i[L],i))}detach(n){let e=this._adjustIndex(n,-1),i=Ed(this._lContainer,e);return i&&fa(sp(this._lContainer),e)!=null?new er(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Fy(t){return t[ma]}function sp(t){return t[ma]||(t[ma]=[])}function f0(t,n){let e,i=n[t.index];return gn(i)?e=i:(e=e0(i,n,null,t),n[t.index]=e,lm(n,e)),hT(e,n,t,i),new Np(e,t,n)}function fT(t,n){let e=t[Me],i=e.createComment(""),r=_n(n,t),o=e.parentNode(r);return bd(e,o,i,e.nextSibling(r),!1),i}var hT=gT,pT=()=>!1;function mT(t,n,e){return pT(t,n,e)}function gT(t,n,e,i){if(t[Ki])return;let r;e.type&8?r=vn(i):r=fT(n,e),t[Ki]=r}var kp=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Op=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)wm(n,e).matches!==null&&this.queries[e].setDirty()}},Md=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=CT(n):this.predicate=n}},Fp=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Pp=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,vT(e,o)),this.matchTNodeWithReadOption(n,e,pd(e,n,o,!1,!1))}else i===en?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,pd(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===j||r===Nt||r===en&&e.type&4)this.addMatch(e.index,-2);else{let o=pd(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function vT(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function _T(t,n){return t.type&11?ss(t,n):t.type&4?Bd(t,n):null}function yT(t,n,e,i){return e===-1?_T(n,t):e===-2?bT(t,n,i):xa(t,t[L],e,n)}function bT(t,n,e){if(e===j)return ss(n,t);if(e===en)return Bd(n,t);if(e===Nt)return f0(n,t)}function h0(t,n,e,i){let r=n[ei].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(yT(n,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function Lp(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=h0(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=Et;f<d.length;f++){let m=d[f];m[Yi]===m[ut]&&Lp(m[L],m,l,i)}if(d[qr]!==null){let f=d[qr];for(let m=0;m<f.length;m++){let g=f[m];Lp(g[L],g,l,i)}}}}}return i}function Em(t,n){return t[ei].queries[n].queryList}function p0(t,n,e){let i=new Gn((e&4)===4);return V_(t,n,i,i.destroy),(n[ei]??=new Op).queries.push(new kp(i))-1}function m0(t,n,e){let i=Ae();return i.firstCreatePass&&(v0(i,new Md(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),p0(i,Z(),n)}function g0(t,n,e,i){let r=Ae();if(r.firstCreatePass){let o=Qe();v0(r,new Md(n,e,i),o.index),ET(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return p0(r,Z(),e)}function CT(t){return t.split(",").map(n=>n.trim())}function v0(t,n,e){t.queries===null&&(t.queries=new Fp),t.queries.track(new Pp(n,e))}function ET(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function wm(t,n){return t.queries.getByIndex(n)}function _0(t,n){let e=t[L],i=wm(e,n);return i.crossesNgTemplate?Lp(e,t,n,[]):h0(e,t,i,n)}function y0(t,n,e){let i,r=Zs(()=>{i._dirtyCounter();let o=wT(i,t);if(n&&o===void 0)throw new x(-951,!1);return o});return i=r[qe],i._dirtyCounter=ee(0),i._flatValue=void 0,r}function Dm(t){return y0(!0,!1,t)}function xm(t){return y0(!0,!0,t)}function b0(t,n){let e=t[qe];e._lView=Z(),e._queryIndex=n,e._queryList=Em(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function wT(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[W]&4)return n?void 0:At;let r=Em(e,i),o=_0(e,i);return r.reset(o,sb),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function nr(t){return!!t&&typeof t.then=="function"}function Sm(t){return!!t&&typeof t.subscribe=="function"}var Si=class{},Ud=class{};var Td=class extends Si{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=g_(n);this._bootstrapComponents=fI(o.bootstrap),this._r3Injector=Qh(n,e,[{provide:Si,useValue:this},...i],da(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Ad=class extends Ud{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Td(this.moduleType,n,[])}};var Ma=class extends Si{injector;instance=null;constructor(n){super();let e=new Hr([...n.providers,{provide:Si,useValue:this}],n.parent||Go(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Pa(t,n,e=null){return new Ma({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var DT=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Ih(!1,e.type),r=i.length>0?Pa([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=P({token:t,providedIn:"environment",factory:()=>new t(M(xe))})}return t})();function R(t){return Aa(()=>{let n=C0(t),e=z(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==zp.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(DT).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Wn.Emulated,styles:t.styles||At,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&tr("NgStandalone"),E0(e);let i=t.dependencies;return e.directiveDefs=Py(i,xT),e.pipeDefs=Py(i,v_),e.id=MT(e),e})}function xT(t){return $i(t)||Eh(t)}function q(t){return Aa(()=>({type:t.type,bootstrap:t.bootstrap||At,declarations:t.declarations||At,imports:t.imports||At,exports:t.exports||At,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function ST(t,n){if(t==null)return Wi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=Vd.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function IT(t){if(t==null)return Wi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function k(t){return Aa(()=>{let n=C0(t);return E0(n),n})}function C0(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Wi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||At,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:ST(t.inputs,n),outputs:IT(t.outputs),debugInfo:null}}function E0(t){t.features?.forEach(n=>n(t))}function Py(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function MT(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var w0=new y("");var Im=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(w0,{optional:!0})??[];injector=u(ae);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ft(this.injector,r);if(nr(o))e.push(o);else if(Sm(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function Mm(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function TT(t){return Object.getPrototypeOf(t.prototype).constructor}function Be(t){let n=TT(t.type),e=!0,i=[t];for(;n;){let r;if(ti(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new x(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=ap(t.inputs),s.declaredInputs=ap(t.declaredInputs),s.outputs=ap(t.outputs);let a=r.hostBindings;a&&OT(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&NT(t,c),l&&kT(t,l),AT(t,r),m_(t.outputs,r.outputs),ti(r)&&r.data.animation){let d=t.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===Be&&(e=!1)}}n=Object.getPrototypeOf(n)}RT(i)}function AT(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function RT(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=ns(r.hostAttrs,e=ns(e,r.hostAttrs))}}function ap(t){return t===Wi?{}:t===At?[]:t}function NT(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function kT(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function OT(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function D0(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=ns(t.mergedAttrs,t.attrs);let d=t.tView=am(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Qo(t,!1);let c=PT(e,n,t,i);id()&&om(e,n,c,t),is(c,n);let l=e0(c,n,c,t);n[i+Ke]=l,lm(n,l),mT(l,t,n)}function FT(t,n,e,i,r,o,s,a,c,l,d){let f=e+Ke,m;return n.firstCreatePass?(m=as(n,f,4,s||null,a||null),Ql()&&c0(n,t,m,Un(n.consts,l),dm),Yy(n,m)):m=n.data[f],D0(m,t,n,e,i,r,o,c),Zo(m)&&jd(n,t,m),l!=null&&Oa(t,m,d),m}function Hd(t,n,e,i,r,o,s,a,c,l,d){let f=e+Ke,m;if(n.firstCreatePass){if(m=as(n,f,4,s||null,a||null),l!=null){let g=Un(n.consts,l);m.localNames=[];for(let b=0;b<g.length;b+=2)m.localNames.push(g[b],-1)}}else m=n.data[f];return D0(m,t,n,e,i,r,o,c),l!=null&&Oa(t,m,d),m}function H(t,n,e,i,r,o,s,a){let c=Z(),l=Ae(),d=Un(l.consts,o);return FT(c,l,t,n,e,i,r,d,void 0,s,a),H}function zd(t,n,e,i,r,o,s,a){let c=Z(),l=Ae(),d=Un(l.consts,o);return Hd(c,l,t,n,e,i,r,d,void 0,s,a),zd}var PT=LT;function LT(t,n,e,i){return _a(!0),n[Me].createComment("")}var $d=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var Tm=new y("");var La=new y("");function x0(){Lf(()=>{let t="";throw new x(600,t)})}var VT=10;var tn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(an);afterRenderManager=u(Fd);zonelessEnabled=u(ba);rootEffectScheduler=u(sd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new I;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(Di);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ne(e=>!e))}constructor(){u(oi,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(xe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=ae.NULL){return this._injector.get(O).run(()=>{if(we(ve.BootstrapComponentStart),!this._injector.get(Im).done){let ie="";throw new x(405,ie)}let a=$i(e),c=this._injector.get(Si),l=new rs(a,c);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:m}=jT(i),g=d||l.selector,b=l.create(r,[],g,c.injector,f,m),S=b.location.nativeElement,N=b.injector.get(Tm,null);return N?.registerApplication(S),b.onDestroy(()=>{this.detachView(b.hostView),Da(this.components,b),N?.unregisterApplication(S)}),this._loadComponent(b),we(ve.BootstrapComponentEnd,b),b})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){we(ve.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Od.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw we(ve.ChangeDetectionEnd),new x(101,!1);let e=G(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,G(e),this.afterTick.next(),we(ve.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(tt,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<VT;){we(ve.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{we(ve.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ga(r))continue;let o=i&&!this.zonelessEnabled?0:1;Kb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>ga(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Da(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(La,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Da(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new x(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function jT(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function Da(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function he(t,n,e,i){let r=Z(),o=Kr();if(Ti(r,o,n)){let s=Ae(),a=Xo();rM(a,r,t,n,e,i)}return he}function _e(t,n,e,i,r,o,s,a){tr("NgControlFlow");let c=Z(),l=Ae(),d=Un(l.consts,o);return Hd(c,l,t,n,e,i,r,d,256,s,a),Am}function Am(t,n,e,i,r,o,s,a){tr("NgControlFlow");let c=Z(),l=Ae(),d=Un(l.consts,o);return Hd(c,l,t,n,e,i,r,d,512,s,a),Am}function ye(t,n){tr("NgControlFlow");let e=Z(),i=Kr(),r=e[i]!==Cn?e[i]:-1,o=r!==-1?Ly(e,Ke+r):void 0,s=0;if(Ti(e,i,t)){let a=G(null);try{if(o!==void 0&&xM(o,s),t!==-1){let c=Ke+t,l=Ly(e,c),d=BT(e[L],c),f=kM(l,d,e),m=mm(e,d,n,{dehydratedView:f});vm(l,m,s,Cd(d,f))}}finally{G(a)}}else if(o!==void 0){let a=DM(o,s);a!==void 0&&(a[Ct]=n)}}function Ly(t,n){return t[n]}function BT(t,n){return ql(t,n)}function E(t,n,e){let i=Z(),r=Kr();if(Ti(i,r,n)){let o=Ae(),s=Xo();zb(s,i,t,n,i[Me],e)}return E}function Vp(t,n,e,i,r){hm(n,t,e,r?"class":"style",i)}function p(t,n,e,i){let r=Z(),o=r[L],s=t+Ke,a=o.firstCreatePass?bm(s,r,2,n,dm,Ql(),e,i):o.data[s];if(wi(a)){let c=r[Bn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(r0(l),()=>(Vy(t,n,r,a,i),p))}}return Vy(t,n,r,a,i),p}function Vy(t,n,e,i,r){if(um(i,e,t,n,S0),Zo(i)){let o=e[L];jd(o,e,i),Gp(o,i,e)}r!=null&&Oa(e,i)}function h(){let t=Ae(),n=Qe(),e=fm(n);return t.firstCreatePass&&Cm(t,e),Uh(e)&&Hh(),jh(),e.classesWithoutHost!=null&&pS(e)&&Vp(t,e,Z(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&mS(e)&&Vp(t,e,Z(),e.stylesWithoutHost,!1),h}function pe(t,n,e,i){return p(t,n,e,i),h(),pe}function ht(t,n,e,i){let r=Z(),o=r[L],s=t+Ke,a=o.firstCreatePass?JM(s,o,2,n,e,i):o.data[s];return um(a,r,t,n,S0),i!=null&&Oa(r,a),ht}function Dt(){let t=Qe(),n=fm(t);return Uh(n)&&Hh(),jh(),Dt}function wn(t,n,e,i){return ht(t,n,e,i),Dt(),wn}var S0=(t,n,e,i,r)=>(_a(!0),wb(n[Me],i,Kh()));function Rm(t,n,e){let i=Z(),r=i[L],o=t+Ke,s=r.firstCreatePass?bm(o,i,8,"ng-container",dm,Ql(),n,e):r.data[o];if(um(s,i,t,"ng-container",UT),Zo(s)){let a=i[L];jd(a,i,s),Gp(a,s,i)}return e!=null&&Oa(i,s),Rm}function Nm(){let t=Ae(),n=Qe(),e=fm(n);return t.firstCreatePass&&Cm(t,e),Nm}function cs(t,n,e){return Rm(t,n,e),Nm(),cs}var UT=(t,n,e,i,r)=>(_a(!0),sI(n[Me],""));function Ue(){return Z()}function Dn(t,n,e){let i=Z(),r=Kr();if(Ti(i,r,n)){let o=Ae(),s=Xo();$b(s,i,t,n,i[Me],e)}return Dn}var Va="en-US";var HT=Va;function I0(t){typeof t=="string"&&(HT=t.toLowerCase().replace(/_/g,"-"))}function F(t,n,e){let i=Z(),r=Ae(),o=Qe();return M0(r,i,i[Me],o,t,n,e),F}function Gd(t,n,e){let i=Z(),r=Ae(),o=Qe();return(o.type&3||e)&&ym(o,r,i,e,i[Me],t,n,eo(o,i,n)),Gd}function M0(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=eo(i,n,o),ym(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let m=d[f],g=d[f+1];c??=eo(i,n,o),Dd(i,n,m,g,r,c)}if(l&&l.length)for(let f of l)c??=eo(i,n,o),Dd(i,n,f,r,r,c)}}function D(t=1){return X_(t)}function zT(t,n){let e=null,i=_I(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Ab(t,o,!0):CI(i,o))return r}return e}function He(t){let n=Z()[Jt][Rt];if(!n.projection){let e=t?t.length:1,i=n.projection=w_(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?zT(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ue(t,n=0,e,i,r,o){let s=Z(),a=Ae(),c=i?t+1:null;c!==null&&Hd(s,a,c,i,r,o,null,e);let l=as(a,Ke+t,16,null,e||null);l.projection===null&&(l.projection=n),Gh();let f=!s[Wo]||Bh();s[Jt][Rt].projection[l.projection]===null&&c!==null?$T(s,a,c):f&&!Nd(l)&&$I(a,s,l)}function $T(t,n,e){let i=Ke+e,r=n.data[i],o=t[i],s=Ap(o,r.tView.ssrId),a=mm(t,r,void 0,{dehydratedView:s});vm(o,a,0,Cd(r,s))}function xn(t,n,e,i){return g0(t,n,e,i),xn}function kt(t,n,e){return m0(t,n,e),kt}function K(t){let n=Z(),e=Ae(),i=Jl();va(i+1);let r=wm(e,i);if(t.dirty&&O_(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=_0(n,i);t.reset(o,sb),t.notifyOnChanges()}return!0}return!1}function Q(){return Em(Z(),Jl())}function Wd(t,n,e,i,r){return b0(n,g0(t,e,i,r)),Wd}function qd(t,n,e,i){return b0(t,m0(n,e,i)),qd}function Yd(t=1){va(Jl()+t)}function Sn(t){let n=H_();return k_(n,Ke+t)}function ud(t,n){return t<<17|n<<2}function ro(t){return t>>17&32767}function GT(t){return(t&2)==2}function WT(t,n){return t&131071|n<<17}function jp(t){return t|2}function os(t){return(t&131068)>>2}function cp(t,n){return t&-131069|n<<2}function qT(t){return(t&1)===1}function Bp(t){return t|1}function YT(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=ro(s),c=os(s);t[i]=e;let l=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||$o(f,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let m=ro(t[a+1]);t[i+1]=ud(m,a),m!==0&&(t[m+1]=cp(t[m+1],i)),t[a+1]=WT(t[a+1],i)}else t[i+1]=ud(a,0),a!==0&&(t[a+1]=cp(t[a+1],i)),a=i;else t[i+1]=ud(c,0),a===0?a=i:t[c+1]=cp(t[c+1],i),c=i;l&&(t[i+1]=jp(t[i+1])),jy(t,d,i,!0),jy(t,d,i,!1),ZT(n,d,t,i,o),s=ud(a,c),o?n.classBindings=s:n.styleBindings=s}function ZT(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&$o(o,n)>=0&&(e[i+1]=Bp(e[i+1]))}function jy(t,n,e,i){let r=t[e+1],o=n===null,s=i?ro(r):os(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];KT(c,n)&&(a=!0,t[s+1]=i?Bp(l):jp(l)),s=i?ro(l):os(l)}a&&(t[e+1]=i?jp(r):Bp(r))}function KT(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?$o(t,n)>=0:!1}var $n={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function QT(t){return t.substring($n.key,$n.keyEnd)}function XT(t){return JT(t),T0(t,A0(t,0,$n.textEnd))}function T0(t,n){let e=$n.textEnd;return e===n?-1:(n=$n.keyEnd=eA(t,$n.key=n,e),A0(t,n,e))}function JT(t){$n.key=0,$n.keyEnd=0,$n.value=0,$n.valueEnd=0,$n.textEnd=t.length}function A0(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function eA(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function so(t,n,e){return R0(t,n,e,!1),so}function ce(t,n){return R0(t,n,null,!0),ce}function cn(t){nA(cA,tA,t,!0)}function tA(t,n){for(let e=XT(n);e>=0;e=T0(n,e))$l(t,QT(n),!0)}function R0(t,n,e,i){let r=Z(),o=Ae(),s=qh(2);if(o.firstUpdatePass&&k0(o,t,s,i),n!==Cn&&Ti(r,s,n)){let a=o.data[Qi()];O0(o,a,r,r[Me],t,r[s+1]=dA(n,e),i,s)}}function nA(t,n,e,i){let r=Ae(),o=qh(2);r.firstUpdatePass&&k0(r,null,o,i);let s=Z();if(e!==Cn&&Ti(s,o,e)){let a=r.data[Qi()];if(F0(a,i)&&!N0(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=Vl(c,e||"")),Vp(r,a,s,e,i)}else lA(r,a,s,s[Me],s[o+1],s[o+1]=aA(t,n,e),i,o)}}function N0(t,n){return n>=t.expandoStartIndex}function k0(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Qi()],s=N0(t,e);F0(o,i)&&n===null&&!s&&(n=!1),n=iA(r,o,n,i),YT(r,o,n,e,s,i)}}function iA(t,n,e,i){let r=Y_(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=lp(null,t,n,e,i),e=Ta(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=lp(r,t,n,e,i),o===null){let c=rA(t,n,i);c!==void 0&&Array.isArray(c)&&(c=lp(null,t,n,c[1],i),c=Ta(c,n.attrs,i),oA(t,n,i,c))}else o=sA(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function rA(t,n,e){let i=e?n.classBindings:n.styleBindings;if(os(i)!==0)return t[ro(i)]}function oA(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[ro(r)]=i}function sA(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Ta(i,s,e)}return Ta(i,n.attrs,e)}function lp(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Ta(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Ta(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),$l(t,s,e?!0:n[++o]))}return t===void 0?null:t}function aA(t,n,e){if(e==null||e==="")return At;let i=[],r=Yn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function cA(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&$l(t,i,e)}function lA(t,n,e,i,r,o,s,a){r===Cn&&(r=At);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let m=c<r.length?r[c+1]:void 0,g=l<o.length?o[l+1]:void 0,b=null,S;d===f?(c+=2,l+=2,m!==g&&(b=f,S=g)):f===null||d!==null&&d<f?(c+=2,b=d):(l+=2,b=f,S=g),b!==null&&O0(t,n,e,i,b,S,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function O0(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],d=qT(l)?By(c,n,e,r,os(l),s):void 0;if(!Rd(d)){Rd(o)||GT(l)&&(o=By(c,null,e,r,a,s));let f=Oh(Qi(),e);WI(i,s,f,r,o)}}function By(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,m=e[r+1];m===Cn&&(m=f?At:void 0);let g=f?Gl(m,i):d===i?m:void 0;if(l&&!Rd(g)&&(g=Gl(c,i)),Rd(g)&&(a=g,s))return a;let b=t[r+1];r=s?ro(b):os(b)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=Gl(c,i))}return a}function Rd(t){return t!==void 0}function dA(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=da(Yn(t)))),t}function F0(t,n){return(t.flags&(n?8:16))!==0}function v(t,n=""){let e=Z(),i=Ae(),r=t+Ke,o=i.firstCreatePass?as(i,r,1,n,null):i.data[r],s=uA(i,e,o,n);e[r]=s,id()&&om(i,e,s,o),Qo(o,!1)}var uA=(t,n,e,i)=>(_a(!0),rI(n[Me],i));function fA(t,n,e,i=""){return Ti(t,Kr(),e)?n+Ul(e)+i:Cn}function Xe(t){return be("",t),Xe}function be(t,n,e){let i=Z(),r=fA(i,t,n,e);return r!==Cn&&hA(i,Qi(),r),be}function hA(t,n,e){let i=Oh(n,t);oI(t[Me],i,e)}function Re(t,n,e){ad(n)&&(n=n());let i=Z(),r=Kr();if(Ti(i,r,n)){let o=Ae(),s=Xo();zb(s,i,t,n,i[Me],e)}return Re}function Pe(t,n){let e=ad(t);return e&&t.set(n),e}function Ne(t,n){let e=Z(),i=Ae(),r=Qe();return M0(i,e,e[Me],r,t,n),Ne}function Uy(t,n,e){let i=Ae();i.firstCreatePass&&P0(n,i.data,i.blueprint,ti(t),e)}function P0(t,n,e,i,r){if(t=bt(t),Array.isArray(t))for(let o=0;o<t.length;o++)P0(t[o],n,e,i,r);else{let o=Ae(),s=Z(),a=Qe(),c=Ur(t)?t:bt(t.provide),l=Th(t),d=a.providerIndexes&1048575,f=a.directiveStart,m=a.providerIndexes>>20;if(Ur(t)||!t.multi){let g=new to(l,r,w,null),b=up(c,n,r?d:d+m,f);b===-1?(pp(yd(a,s),o,c),dp(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(g),s.push(g)):(e[b]=g,s[b]=g)}else{let g=up(c,n,d+m,f),b=up(c,n,d,d+m),S=g>=0&&e[g],N=b>=0&&e[b];if(r&&!N||!r&&!S){pp(yd(a,s),o,c);let ie=gA(r?mA:pA,e.length,r,i,l,t);!r&&N&&(e[b].providerFactory=ie),dp(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(ie),s.push(ie)}else{let ie=L0(e[r?b:g],l,!r&&i);dp(o,t,g>-1?g:b,ie)}!r&&i&&N&&e[b].componentProviders++}}}function dp(t,n,e,i){let r=Ur(n),o=T_(n);if(r||o){let c=(o?bt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function L0(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function up(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function pA(t,n,e,i,r){return Up(this.multi,[])}function mA(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=xa(i,i[L],this.providerFactory.index,r);s=c.slice(0,a),Up(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],Up(o,s);return s}function Up(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function gA(t,n,e,i,r,o){let s=new to(t,e,w,null);return s.multi=[],s.index=n,s.componentProviders=0,L0(s,r,i&&!e),s}function ze(t,n){return e=>{e.providersResolver=(i,r)=>Uy(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Uy(i,r?r(n):n,!0))}}function ao(t,n,e){return _A(Z(),z_(),t,n,e)}function vA(t,n){let e=t[n];return e===Cn?void 0:e}function _A(t,n,e,i,r,o){let s=n+e;return Ti(t,s,r)?FM(t,s+1,o?i.call(o,r):i(r)):vA(t,s+1)}function ja(t,n){return Bd(t,n)}var V0=(()=>{class t{applicationErrorHandler=u(an);appRef=u(tn);taskService=u(Di);ngZone=u(O);zonelessEnabled=u(ba);tracing=u(oi,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new fe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ca):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(np,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ny:Xh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ca+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function j0(){return[{provide:Jn,useExisting:V0},{provide:O,useClass:la},{provide:ba,useValue:!0}]}var km=(()=>{class t{compileModuleSync(e){return new Ad(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function yA(){return typeof $localize<"u"&&$localize.locale||Va}var Zd=new y("",{factory:()=>u(Zd,{optional:!0,skipSelf:!0})||yA()});function Yt(t,n){return Zs(t,n?.equal)}function ge(t){return Pv(t)}var bA=t=>t;function Om(t,n){if(typeof t=="function"){let e=Hf(t,bA,n?.equal);return B0(e,n?.debugName)}else{let e=Hf(t.source,t.computation,t.equal);return B0(e,t.debugName)}}function B0(t,n){let e=t[qe],i=t;return i.set=r=>Ov(e,r),i.update=r=>Fv(e,r),i.asReadonly=rd.bind(t),i}var Y0=Symbol("InputSignalNode#UNSET"),VA=z(C({},Ks),{transformFn:void 0,applyValueToInputSignal(t,n){Tr(t,n)}});function Z0(t,n){let e=Object.create(VA);e.value=t,e.transformFn=n?.transform;function i(){if(Bi(e),e.value===Y0){let r=null;throw new x(-950,r)}return e.value}return i[qe]=e,i}var ir=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Ra(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},K0=(()=>{let t=new y("");return t.__NG_ELEMENT_ID__=n=>{let e=Qe();if(e===null)throw new x(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new x(-204,!1)},t})();function Hm(t){return jA(t)?t.default:t}function jA(t){return t&&typeof t=="object"&&"default"in t}function U0(t,n){return Z0(t,n)}function BA(t){return Z0(Y0,t)}var ls=(U0.required=BA,U0);function H0(t,n){return Dm(n)}function UA(t,n){return xm(n)}var Ua=(H0.required=UA,H0);function z0(t,n){return Dm(n)}function HA(t,n){return xm(n)}var Q0=(z0.required=HA,z0);var zA=1e4;var CW=zA-1e3;var Pm=class{supports(n){return _m(n)}create(n){return new Lm(n)}},$A=(t,n)=>n,Lm=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||$A}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<$0(i,r,o)?e:i,a=$0(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let m=0;m<l;m++){let g=m<o.length?o[m]:o[m]=0,b=g+m;d<=b&&b<l&&(o[m]=g+1)}let f=s.previousIndex;o[f]=d-l}}a!==c&&n(s,a,c)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!_m(n))throw new x(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,o0(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new Vm(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Qd),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Qd),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Vm=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},jm=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Qd=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new jm,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function $0(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function G0(){return new Xd([new Pm])}var Xd=(()=>{class t{factories;static \u0275prov=P({token:t,providedIn:"root",factory:G0});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=u(t,{optional:!0,skipSelf:!0});return t.create(e,i||G0())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new x(901,!1)}}return t})();var me=(()=>{class t{static __NG_ELEMENT_ID__=GA}return t})();function GA(t){return WA(Qe(),Z(),(t&16)===16)}function WA(t,n,e){if(wi(t)&&!e){let i=yn(t.index,n);return new er(i,i)}else if(t.type&175){let i=n[Jt];return new er(i,n)}return null}var Bm=new y(""),qA=new y("");function Ba(t){return!t.moduleRef}function YA(t){let n=Ba(t)?t.r3Injector:t.moduleRef.injector,e=n.get(O);return e.run(()=>{Ba(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(an),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ba(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Bm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Bm);s.add(o),t.moduleRef.onDestroy(()=>{Da(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return KA(i,e,()=>{let o=n.get(Di),s=o.add(),a=n.get(Im);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(Zd,Va);if(I0(c||Va),!n.get(qA,!0))return Ba(t)?n.get(tn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ba(t)){let d=n.get(tn);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return ZA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var ZA;function KA(t,n,e){try{let i=e();return nr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Kd=null;function QA(t=[],n){return ae.create({name:n,providers:[{provide:pa,useValue:"platform"},{provide:Bm,useValue:new Set([()=>Kd=null])},...t]})}function XA(t=[]){if(Kd)return Kd;let n=QA(t);return Kd=n,x0(),JA(n),n}function JA(t){let n=t.get(od,null);ft(t,()=>{n?.forEach(e=>e())})}function X0(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;we(ve.BootstrapApplicationStart);try{let o=r?.injector??XA(i),s=[j0(),ry,...e||[]],a=new Ma({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return YA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{we(ve.BootstrapApplicationEnd)}}function B(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function In(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Fm=Symbol("NOT_SET"),J0=new Set,eR=z(C({},Ks),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Fm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Fm&&!No(this))return this.signal;try{for(let r of this.cleanup??J0)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=hi(this),i;try{i=this.userFn.apply(null,n)}finally{Ui(this,e)}return(this.value===Fm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Um=class extends Sa{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(wt),s),this.scheduler=r;for(let a of tm){let c=e[a];if(c===void 0)continue;let l=Object.create(eR);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Bi(l),l.value),l.signal[qe]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??J0)e()}finally{Hi(n)}}};function eC(t,n){let e=n?.injector??u(ae),i=e.get(Jn),r=e.get(Fd),o=e.get(oi,null,{optional:!0});r.impl??=e.get(nm);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Jo,null,{optional:!0}),c=new Um(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Jd(t,n){let e=$i(t),i=n.elementInjector||Go();return new rs(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var tC=null;function Mn(){return tC}function zm(t){tC??=t}var Ha=class{},ds=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:()=>u(nC),providedIn:"platform"})}return t})();var nC=(()=>{class t extends ds{_location;_history;_doc=u(U);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Mn().getBaseHref(this._doc)}onPopState(e){let i=Mn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Mn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function oC(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function iC(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function rr(t){return t&&t[0]!=="?"?`?${t}`:t}var us=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:()=>u(nR),providedIn:"root"})}return t})(),tR=new y(""),nR=(()=>{class t extends us{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(U).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return oC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+rr(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+rr(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+rr(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(M(ds),M(tR,8))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var or=(()=>{class t{_subject=new I;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=oR(iC(rC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+rr(i))}normalize(e){return t.stripTrailingSlash(rR(this._basePath,rC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+rr(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+rr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=rr;static joinWithSlash=oC;static stripTrailingSlash=iC;static \u0275fac=function(i){return new(i||t)(M(us))};static \u0275prov=P({token:t,factory:()=>iR(),providedIn:"root"})}return t})();function iR(){return new or(M(us))}function rR(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function rC(t){return t.replace(/\/index\.html$/,"")}function oR(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var eu=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},nn=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new eu(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),sC(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);sC(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(w(Nt),w(en),w(Xd))};static \u0275dir=k({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function sC(t,n){t.context.$implicit=n.item}var lt=(()=>{class t{_viewContainer;_context=new tu;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){aC(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){aC(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(w(Nt),w(en))};static \u0275dir=k({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),tu=class{$implicit=null;ngIf=null};function aC(t,n){if(t&&!t.createEmbeddedView)throw new x(2020,!1)}var za=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(ae);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(w(Nt))};static \u0275dir=k({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[nt]})}return t})();var it=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})();function $a(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var $m="browser";function cC(t){return t===$m}var Ga=class{_doc;constructor(n){this._doc=n}manager},nu=(()=>{class t extends Ga{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=P({token:t,factory:t.\u0275fac})}return t})(),ou=new y(""),Ym=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof nu));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof nu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new x(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(M(ou),M(O))};static \u0275prov=P({token:t,factory:t.\u0275fac})}return t})(),Gm="ng-app-id";function lC(t){for(let n of t)n.remove()}function dC(t,n){let e=n.createElement("style");return e.textContent=t,e}function aR(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Gm}="${n}"],link[${Gm}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Gm),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function qm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Zm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,aR(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,dC);i?.forEach(r=>this.addUsage(r,this.external,qm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(lC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])lC(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,dC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,qm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(M(U),M(Qr),M(Jr,8),M(Xr))};static \u0275prov=P({token:t,factory:t.\u0275fac})}return t})(),Wm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Km=/%COMP%/g;var fC="%COMP%",cR=`_nghost-${fC}`,lR=`_ngcontent-${fC}`,dR=!0,uR=new y("",{factory:()=>dR});function fR(t){return lR.replace(Km,t)}function hR(t){return cR.replace(Km,t)}function hC(t,n){return n.map(e=>e.replace(Km,t))}var Qm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Wa(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof ru?r.applyToHost(e):r instanceof qa&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Wn.Emulated:o=new ru(c,l,i,this.appId,d,s,a,f);break;case Wn.ShadowDom:return new iu(c,e,i,s,a,this.nonce,f,l);case Wn.ExperimentalIsolatedShadowDom:return new iu(c,e,i,s,a,this.nonce,f);default:o=new qa(c,l,i,d,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(M(Ym),M(oo),M(Qr),M(uR),M(U),M(O),M(Jr),M(oi,8))};static \u0275prov=P({token:t,factory:t.\u0275fac})}return t})(),Wa=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Wm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(uC(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(uC(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new x(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Wm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Wm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(ri.DashCase|ri.Important)?n.style.setProperty(e,i,r&ri.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&ri.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Mn().getGlobalEventTarget(this.doc,n),!n))throw new x(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function uC(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var iu=class extends Wa{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=hC(i.id,l);for(let f of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=f,this.shadowRoot.appendChild(m)}let d=i.getExternalStyles?.();if(d)for(let f of d){let m=qm(f,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},qa=class extends Wa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?hC(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&no.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},ru=class extends qa{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=fR(l),this.hostAttr=hR(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var su=class t extends Ha{supportsDOMEvents=!0;static makeCurrent(){zm(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=pR();return e==null?null:mR(e)}resetBaseElement(){Ya=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return $a(document.cookie,n)}},Ya=null;function pR(){return Ya=Ya||document.head.querySelector("base"),Ya?Ya.getAttribute("href"):null}function mR(t){return new URL(t,document.baseURI).pathname}var pC=["alt","control","meta","shift"],gR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},vR={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},mC=(()=>{class t extends Ga{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Mn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),pC.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=gR[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),pC.forEach(s=>{if(s!==r){let a=vR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=P({token:t,factory:t.\u0275fac})}return t})();function Xm(t,n,e){return De(this,null,function*(){let i=C({rootComponent:t},_R(n,e));return X0(i)})}function _R(t,n){return{platformRef:n?.platformRef,appProviders:[...wR,...t?.providers??[]],platformProviders:ER}}function yR(){su.makeCurrent()}function bR(){return new qt}function CR(){return $p(document),document}var ER=[{provide:Xr,useValue:$m},{provide:od,useValue:yR,multi:!0},{provide:U,useFactory:CR}];var wR=[{provide:pa,useValue:"root"},{provide:qt,useFactory:bR},{provide:ou,useClass:nu,multi:!0},{provide:ou,useClass:mC,multi:!0},Qm,{provide:oo,useClass:Zm},{provide:Zm,useExisting:oo},Ym,{provide:tt,useExisting:Qm},[]];var Ni=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var cu=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},lu=class{encodeKey(n){return gC(n)}encodeValue(n){return gC(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function DR(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var xR=/%(\d[a-f0-9])/gi,SR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function gC(t){return encodeURIComponent(t).replace(xR,(n,e)=>SR[e]??n)}function au(t){return`${t}`}var Ri=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new lu,n.fromString){if(n.fromObject)throw new x(2805,!1);this.map=DR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(au):[au(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(au(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(au(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function IR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function vC(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function _C(t){return typeof Blob<"u"&&t instanceof Blob}function yC(t){return typeof FormData<"u"&&t instanceof FormData}function MR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Jm="Content-Type",bC="Accept",EC="text/plain",wC="application/json",TR=`${wC}, ${EC}, */*`,fs=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(IR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new x(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Ni,this.context??=new cu,!this.params)this.params=new Ri,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||vC(this.body)||_C(this.body)||yC(this.body)||MR(this.body)?this.body:this.body instanceof Ri?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||yC(this.body)?null:_C(this.body)?this.body.type||null:vC(this.body)?null:typeof this.body=="string"?EC:this.body instanceof Ri?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?wC:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,m=n.integrity||this.integrity,g=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,S=n.timeout??this.timeout,N=n.body!==void 0?n.body:this.body,ie=n.withCredentials??this.withCredentials,st=n.reportProgress??this.reportProgress,Tt=n.reportUploadProgress??this.reportUploadProgress,To=n.reportDownloadProgress??this.reportDownloadProgress,Gs=n.headers||this.headers,wr=n.params||this.params,Yc=n.context??this.context;return n.setHeaders!==void 0&&(Gs=Object.keys(n.setHeaders).reduce((Ao,Dr)=>Ao.set(Dr,n.setHeaders[Dr]),Gs)),n.setParams&&(wr=Object.keys(n.setParams).reduce((Ao,Dr)=>Ao.set(Dr,n.setParams[Dr]),wr)),new t(e,i,N,{params:wr,headers:Gs,context:Yc,reportProgress:st,reportUploadProgress:Tt,reportDownloadProgress:To,responseType:r,withCredentials:ie,transferCache:b,keepalive:o,cache:a,priority:s,timeout:S,mode:c,redirect:l,credentials:d,referrer:f,integrity:m,referrerPolicy:g})}},hs=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(hs||{}),ps=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Ni,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},du=class t extends ps{constructor(n={}){super(n)}type=hs.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Za=class t extends ps{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=hs.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},co=class extends ps{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},AR=200;var RR=/^\)\]\}',?\n/,Lq=1024*1024,DC=new y("",{factory:()=>null}),uu=(()=>{class t{fetchImpl=u(tg,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(O);destroyRef=u(wt);maxResponseSize=u(DC);handle(e){return new re(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(ng,s=>i.error(new co({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return De(this,null,function*(){let o=this.createRequestInit(e),s;try{let N=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));NR(N),r.next({type:hs.Sent}),s=yield N}catch(N){r.error(new co({error:N,status:N.status??0,statusText:N.statusText,url:e.urlWithParams,headers:N.headers}));return}let a=new Ni(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,f=null,m=e.reportProgress||e.reportDownloadProgress;if(m&&r.next(new du({headers:a,status:d,statusText:c,url:l})),s.body){let N=s.headers.get("content-length"),ie=N!==null?Number(N):NaN;this.maxResponseSize!==null&&Number.isFinite(ie)&&ie>this.maxResponseSize&&CC(this.maxResponseSize);let st=[],Tt=s.body.getReader(),To=0,Gs,wr,Yc=typeof Zone<"u"&&Zone.current,Ao=!1;if(yield this.ngZone.runOutsideAngular(()=>De(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield Tt.cancel(),Ao=!0;break}let{done:Ws,value:kf}=yield Tt.read();if(Ws)break;if(st.push(kf),To+=kf.length,this.maxResponseSize!==null&&To>this.maxResponseSize&&(yield Tt.cancel(),CC(this.maxResponseSize)),m){wr=e.responseType==="text"?(wr??"")+(Gs??=new TextDecoder).decode(kf,{stream:!0}):void 0;let wv=()=>r.next({type:hs.DownloadProgress,total:Number.isFinite(ie)?ie:void 0,loaded:To,partialText:wr});Yc?Yc.run(wv):wv()}}})),Ao){r.complete();return}let Dr=this.concatChunks(st,To);try{let Ws=s.headers.get(Jm)??"";f=this.parseBody(e,Dr,Ws,d)}catch(Ws){r.error(new co({error:Ws,headers:new Ni(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=f?AR:0);let g=d>=200&&d<300,b=s.redirected,S=s.type;g?(r.next(new Za({body:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:S})),r.complete()):r.error(new co({error:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:S}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(RR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new x(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(bC)||(i[bC]=TR),!e.headers.has(Jm)){let o=e.detectContentTypeHeader();o!==null&&(i[Jm]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),tg=class{};function ng(){}function NR(t){t.then(ng,ng)}function CC(t){throw new x(-2825,!1)}function kR(t,n){return n(t)}function OR(t,n,e){return(i,r)=>ft(e,()=>n(i,o=>t(o,r)))}var ig=new y("",{factory:()=>[]}),xC=new y(""),SC=new y("",{factory:()=>!0});var rg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(uu),r},providedIn:"root"})}return t})();var fu=(()=>{class t{backend;injector;chain=null;pendingTasks=u(Ca);contributeToStability=u(SC);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(ig),...this.injector.get(xC,[])]));this.chain=i.reduceRight((r,o)=>OR(r,o,this.injector),kR)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Lr(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(M(rg),M(xe))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),og=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(fu),r},providedIn:"root"})}return t})();function eg(t,n){return C({body:n},t)}var Tn=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof fs)o=e;else{let c;r.headers instanceof Ni?c=r.headers:c=new Ni(r.headers);let l;r.params&&(r.params instanceof Ri?l=r.params:l=new Ri({fromObject:r.params})),o=new fs(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=V(o).pipe(Uo(c=>this.handler.handle(c)));if(e instanceof fs||r.observe==="events")return s;let a=s.pipe(Ie(c=>c instanceof Za));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new x(2806,!1);return c.body}));case"blob":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new x(2807,!1);return c.body}));case"text":return a.pipe(ne(c=>{if(c.body!==null&&typeof c.body!="string")throw new x(2808,!1);return c.body}));default:return a.pipe(ne(c=>c.body))}case"response":return a;default:throw new x(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Ri().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,eg(r,i))}post(e,i,r={}){return this.request("POST",e,eg(r,i))}put(e,i,r={}){return this.request("PUT",e,eg(r,i))}static \u0275fac=function(i){return new(i||t)(M(og))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var FR=new y("",{factory:()=>!0}),PR="XSRF-TOKEN",LR=new y("",{factory:()=>PR}),VR="X-XSRF-TOKEN",jR=new y("",{factory:()=>VR}),BR=(()=>{class t{cookieName=u(LR);doc=u(U);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=$a(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),IC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(BR),r},providedIn:"root"})}return t})();function UR(t,n){if(!u(FR)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(ds).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch(r){return n(t)}let e=u(IC).getToken(),i=u(jR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var sg=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t[t.Xhr=7]="Xhr",t})(sg||{});function HR(t,n){return{\u0275kind:t,\u0275providers:n}}function ag(...t){let n=[Tn,uu,fu,{provide:og,useExisting:fu},{provide:rg,useFactory:()=>u(uu)},{provide:ig,useValue:UR,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return yi(n)}function cg(t){return HR(sg.Interceptors,t.map(n=>({provide:ig,useValue:n,multi:!0})))}var MC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Qa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=P({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(zR),r},providedIn:"root"})}return t})(),zR=(()=>{class t extends Qa{_doc=u(U);sanitize(e,i){if(i==null)return null;switch(e){case ct.NONE:return i;case ct.HTML:return Ii(i,"HTML")?Yn(i):Qp(this._doc,String(i)).toString();case ct.STYLE:return Ii(i,"Style")?Yn(i):i;case ct.SCRIPT:if(Ii(i,"Script"))return Yn(i);throw new x(5200,!1);case ct.URL:return Ii(i,"URL")?Yn(i):Na(String(i));case ct.RESOURCE_URL:if(Ii(i,"ResourceURL"))return Yn(i);throw new x(5201,!1);default:throw new x(5202,!1)}}bypassSecurityTrustHtml(e){return Wp(e)}bypassSecurityTrustStyle(e){return qp(e)}bypassSecurityTrustScript(e){return Yp(e)}bypassSecurityTrustUrl(e){return Zp(e)}bypassSecurityTrustResourceUrl(e){return Kp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var X="primary",uc=Symbol("RouteTitle"),hg=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function uo(t){return new hg(t)}function lg(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function VC(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return lg(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!lg(o,t.slice(0,o.length),a)||!lg(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function _u(t){return new Promise((n,e)=>{t.pipe(mi()).subscribe({next:i=>n(i),error:i=>e(i)})})}function $R(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!ai(t[e],n[e]))return!1;return!0}function ai(t,n){let e=t?pg(t):void 0,i=n?pg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!jC(t[r],n[r]))return!1;return!0}function pg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function jC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function GR(t){return t.length>0?t[t.length-1]:null}function po(t){return na(t)?t:nr(t)?Le(Promise.resolve(t)):V(t)}function BC(t){return na(t)?_u(t):Promise.resolve(t)}var WR={exact:zC,subset:$C},UC={exact:qR,subset:YR,ignored:()=>!0},HC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},mg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function AC(t,n,e){return WR[e.paths](t.root,n.root,e.matrixParams)&&UC[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function qR(t,n){return ai(t,n)}function zC(t,n,e){if(!lo(t.segments,n.segments)||!mu(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!zC(t.children[i],n.children[i],e))return!1;return!0}function YR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>jC(t[e],n[e]))}function $C(t,n,e){return GC(t,n,n.segments,e)}function GC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!lo(r,e)||n.hasChildren()||!mu(r,e,i))}else if(t.segments.length===e.length){if(!lo(t.segments,e)||!mu(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!$C(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!lo(t.segments,r)||!mu(t.segments,r,i)||!t.children[X]?!1:GC(t.children[X],n,o,i)}}function mu(t,n,e){return n.every((i,r)=>UC[e](t[r].parameters,i.parameters))}var dn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Ce([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=uo(this.queryParams),this._queryParamMap}toString(){return QR.serialize(this)}},Ce=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return gu(this)}},sr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=uo(this.parameters),this._parameterMap}toString(){return qC(this)}};function ZR(t,n){return lo(t,n)&&t.every((e,i)=>ai(e.parameters,n[i].parameters))}function lo(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function KR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===X&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==X&&(e=e.concat(n(r,i)))}),e}var ws=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>new ar})}return t})(),ar=class{parse(n){let e=new vg(n);return new dn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Xa(n.root,!0)}`,i=eN(n.queryParams),r=typeof n.fragment=="string"?`#${XR(n.fragment)}`:"";return`${e}${i}${r}`}},QR=new ar;function gu(t){return t.segments.map(n=>qC(n)).join("/")}function Xa(t,n){if(!t.hasChildren())return gu(t);if(n){let e=t.children[X]?Xa(t.children[X],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==X&&i.push(`${r}:${Xa(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=KR(t,(i,r)=>r===X?[Xa(t.children[X],!1)]:[`${r}:${Xa(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[X]!=null?`${gu(t)}/${e[0]}`:`${gu(t)}/(${e.join("//")})`}}function WC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function hu(t){return WC(t).replace(/%3B/gi,";")}function XR(t){return encodeURI(t)}function gg(t){return WC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function vu(t){return decodeURIComponent(t)}function RC(t){return vu(t.replace(/\+/g,"%20"))}function qC(t){return`${gg(t.path)}${JR(t.parameters)}`}function JR(t){return Object.entries(t).map(([n,e])=>`;${gg(n)}=${gg(e)}`).join("")}function eN(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${hu(e)}=${hu(r)}`).join("&"):`${hu(e)}=${hu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var tN=/^[^\/()?;#]+/;function dg(t){let n=t.match(tN);return n?n[0]:""}var nN=/^[^\/()?;=#]+/;function iN(t){let n=t.match(nN);return n?n[0]:""}var rN=/^[^=?&#]+/;function oN(t){let n=t.match(rN);return n?n[0]:""}var sN=/^[^&#]+/;function aN(t){let n=t.match(sN);return n?n[0]:""}var vg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Ce([],{}):new Ce([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[X]=new Ce(e,i)),r}parseSegment(){let n=dg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(n),new sr(vu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=iN(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=dg(this.remaining);r&&(i=r,this.capture(i))}n[vu(e)]=vu(i)}parseQueryParam(n){let e=oN(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=aN(this.remaining);s&&(i=s,this.capture(i))}let r=RC(e),o=RC(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=dg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new x(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=X);let a=this.parseChildren(e+1);i[s??X]=Object.keys(a).length===1&&a[X]?a[X]:new Ce([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new x(4011,!1)}};function YC(t){return t.segments.length>0?new Ce([],{[X]:t}):t}function ZC(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=ZC(r);if(i===X&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Ce(t.segments,n);return cN(e)}function cN(t){if(t.numberOfChildren===1&&t.children[X]){let n=t.children[X];return new Ce(t.segments.concat(n.segments),n.children)}return t}function cr(t){return t instanceof dn}function KC(t,n,e=null,i=null,r=new ar){let o=QC(t);return XC(o,n,e,i,r)}function QC(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new Ce(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=YC(i);return n??r}function XC(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return ug(o,o,o,e,i,r);let s=lN(n);if(s.toRoot())return ug(o,o,new Ce([],{}),e,i,r);let a=dN(s,o,t),c=a.processChildren?ec(a.segmentGroup,a.index,s.commands):eE(a.segmentGroup,a.index,s.commands);return ug(o,a.segmentGroup,c,e,i,r)}function yu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function ic(t){return typeof t=="object"&&t!=null&&t.outlets}function NC(t,n,e){t||="\u0275";let i=new dn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function ug(t,n,e,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>NC(l,f,o)):NC(l,d,o);let a;t===n?a=e:a=JC(t,n,e);let c=YC(ZC(a));return new dn(c,s,r)}function JC(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=JC(o,n,e)}),new Ce(t.segments,i)}var bu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&yu(i[0]))throw new x(4003,!1);let r=i.find(ic);if(r&&r!==GR(i))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function lN(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new bu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new bu(e,n,i)}var gs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function dN(t,n,e){if(t.isAbsolute)return new gs(n,!0,0);if(!e)return new gs(n,!1,NaN);if(e.parent===null)return new gs(e,!0,0);let i=yu(t.commands[0])?0:1,r=e.segments.length-1+i;return uN(e,r,t.numberOfDoubleDots)}function uN(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new x(4005,!1);r=i.segments.length}return new gs(i,!1,r-o)}function fN(t){return ic(t[0])?t[0].outlets:{[X]:t}}function eE(t,n,e){if(t??=new Ce([],{}),t.segments.length===0&&t.hasChildren())return ec(t,n,e);let i=hN(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Ce(t.segments.slice(0,i.pathIndex),{});return o.children[X]=new Ce(t.segments.slice(i.pathIndex),t.children),ec(o,0,r)}else return i.match&&r.length===0?new Ce(t.segments,{}):i.match&&!t.hasChildren()?_g(t,n,e):i.match?ec(t,0,r):_g(t,n,e)}function ec(t,n,e){if(e.length===0)return new Ce(t.segments,{});{let i=fN(e),r={};if(Object.keys(i).some(o=>o!==X)&&t.children[X]&&t.numberOfChildren===1&&t.children[X].segments.length===0){let o=ec(t.children[X],n,e);return new Ce(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=eE(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Ce(t.segments,r)}}function hN(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(ic(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!OC(c,l,s))return o;i+=2}else{if(!OC(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function _g(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(ic(o)){let c=pN(o.outlets);return new Ce(i,c)}if(r===0&&yu(e[0])){let c=t.segments[n];i.push(new sr(c.path,kC(e[0]))),r++;continue}let s=ic(o)?o.outlets[X]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&yu(a)?(i.push(new sr(s,kC(a))),r+=2):(i.push(new sr(s,{})),r++)}return new Ce(i,{})}function pN(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=_g(new Ce([],{}),0,i))}),n}function kC(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function OC(t,n,e){return t==e.path&&ai(n,e.parameters)}var tc="imperative",pt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(pt||{}),un=class{id;url;constructor(n,e){this.id=n,this.url=e}},fo=class extends un{type=pt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ci=class extends un{urlAfterRedirects;type=pt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Ot=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Ot||{}),rc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(rc||{}),An=class extends un{reason;code;type=pt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function tE(t){return t instanceof An&&(t.code===Ot.Redirect||t.code===Ot.SupersededByNewNavigation)}var Oi=class extends un{reason;code;type=pt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ho=class extends un{error;target;type=pt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},oc=class extends un{urlAfterRedirects;state;type=pt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Cu=class extends un{urlAfterRedirects;state;type=pt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Eu=class extends un{urlAfterRedirects;state;shouldActivate;type=pt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},wu=class extends un{urlAfterRedirects;state;type=pt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Du=class extends un{urlAfterRedirects;state;type=pt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},xu=class{route;type=pt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Su=class{route;type=pt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Iu=class{snapshot;type=pt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Mu=class{snapshot;type=pt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Tu=class{snapshot;type=pt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Au=class{snapshot;type=pt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var _s=class{},sc=class{},ys=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function mN(t){return!(t instanceof _s)&&!(t instanceof ys)&&!(t instanceof sc)}var Ru=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Ds(this.rootInjector)}},Ds=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Ru(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(M(xe))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Nu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=yg(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=yg(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=bg(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return bg(n,this._root).map(e=>e.value)}};function yg(t,n){if(t===n.value)return n;for(let e of n.children){let i=yg(t,e);if(i)return i}return null}function bg(t,n){if(t===n.value)return[n];for(let e of n.children){let i=bg(t,e);if(i.length)return i.unshift(n),i}return[]}var ln=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ms(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var ac=class extends Nu{snapshot;constructor(n,e){super(n),this.snapshot=e,Tg(this,n)}toString(){return this.snapshot.toString()}};function nE(t,n){let e=gN(t,n),i=new at([new sr("",{})]),r=new at({}),o=new at({}),s=new at({}),a=new at(""),c=new Zt(i,r,s,a,o,X,t,e.root);return c.snapshot=e.root,new ac(new ln(c,[]),e)}function gN(t,n){let e={},i={},r={},s=new bs([],e,r,"",i,X,t,null,{},n);return new cc("",new ln(s,[]))}var Zt=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ne(l=>l[uc]))??V(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ne(n=>uo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ne(n=>uo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},vN="always";function Mg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&rE(r)&&(i.resolve[uc]=r.title),i}var bs=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[uc]}constructor(n,e,i,r,o,s,a,c,l,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=uo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=uo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},cc=class extends Nu{url;constructor(n,e){super(e),this.url=n,Tg(this,e)}toString(){return iE(this._root)}};function Tg(t,n){n.value._routerState=t,n.children.forEach(e=>Tg(t,e))}function iE(t){let n=t.children.length>0?` { ${t.children.map(iE).join(", ")} } `:"";return`${t.value}${n}`}function fg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,ai(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),ai(n.params,e.params)||t.paramsSubject.next(e.params),$R(n.url,e.url)||t.urlSubject.next(e.url),ai(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Cg(t,n){let e=ai(t.params,n.params)&&ZR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Cg(t.parent,n.parent))}function rE(t){return typeof t.title=="string"||t.title===null}var oE=new y(""),fc=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=X;activateEvents=new Y;deactivateEvents=new Y;attachEvents=new Y;detachEvents=new Y;routerOutletData=ls();parentContexts=u(Ds);location=u(Nt);changeDetector=u(me);inputBinder=u(Pu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Eg(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[nt]})}return t})(),Eg=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Zt?this.route:n===Ds?this.childContexts:n===oE?this.outletData:this.parent.get(n,e)}},Pu=new y("");var Ag=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&pe(0,"router-outlet")},dependencies:[fc],encapsulation:2,changeDetection:1})}return t})();function Rg(t){let n=t.children&&t.children.map(Rg),e=n?z(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==X&&(e.component=Ag),e}function _N(t,n,e){let i=new Set,r=lc(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new ac(r,n)}}function lc(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=yN(t,n,e,i);return new ln(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>lc(t,c,void 0,i)),a}}let r=bN(n.value);i.add(r);let o=n.children.map(s=>lc(t,s,void 0,i));return new ln(r,o)}}function yN(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return lc(t,r,o,i);return lc(t,r,void 0,i)})}function bN(t){return new Zt(new at(t.url),new at(t.params),new at(t.queryParams),new at(t.fragment),new at(t.data),t.outlet,t.component,t)}var Cs=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},sE="ngNavigationCancelingError";function ku(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=cr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=aE(!1,Ot.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function aE(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[sE]=!0,e.cancellationCode=n,e}function CN(t){return cE(t)&&cr(t.url)}function cE(t){return!!t&&t[sE]}var wg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),fg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ms(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ms(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ms(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=ms(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Au(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Mu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(fg(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),fg(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Ou=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},vs=class{component;route;constructor(n,e){this.component=n,this.route=e}};function EN(t,n,e){let i=t._root,r=n?n._root:null;return Ja(i,r,e,[i.value])}function wN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function xs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!gh(t)?t:n.get(t):i}function Ja(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ms(n);return t.children.forEach(s=>{DN(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>nc(a,e.getContext(s),r)),r}function DN(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=xN(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Ou(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Ja(t,n,a?a.children:null,i,r):Ja(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new vs(a.outlet.component,s))}else s&&nc(n,a,r),r.canActivateChecks.push(new Ou(i)),o.component?Ja(t,null,a?a.children:null,i,r):Ja(t,null,e,i,r);return r}function xN(t,n,e){if(typeof e=="function")return ft(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!lo(t.url,n.url);case"pathParamsOrQueryParamsChange":return!lo(t.url,n.url)||!ai(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Cg(t,n)||!ai(t.queryParams,n.queryParams);default:return!Cg(t,n)}}function nc(t,n,e){let i=ms(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?nc(s,n.children.getContext(o),e):nc(s,null,e):nc(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new vs(n.outlet.component,r)):e.canDeactivateChecks.push(new vs(null,r)):e.canDeactivateChecks.push(new vs(null,r))}function hc(t){return typeof t=="function"}function SN(t){return typeof t=="boolean"}function IN(t){return t&&hc(t.canLoad)}function MN(t){return t&&hc(t.canActivate)}function TN(t){return t&&hc(t.canActivateChild)}function AN(t){return t&&hc(t.canDeactivate)}function RN(t){return t&&hc(t.canMatch)}function lE(t){return t instanceof Or||t?.name==="EmptyError"}var pu=Symbol("INITIAL_VALUE");function Es(){return We(t=>Xf(t.map(n=>n.pipe(Wt(1),yt(pu)))).pipe(ne(n=>{for(let e of n)if(e!==!0){if(e===pu)return pu;if(e===!1||NN(e))return e}return!0}),Ie(n=>n!==pu),Wt(1)))}function NN(t){return cr(t)||t instanceof Cs}function dE(t){return t.aborted?V(void 0).pipe(Wt(1)):new re(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function uE(t){return ke(dE(t))}function kN(t){return Gt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?V(z(C({},n),{guardsResult:!0})):ON(o,e,i).pipe(Gt(s=>s&&SN(s)?FN(e,r,t):V(s)),ne(s=>z(C({},n),{guardsResult:s})))})}function ON(t,n,e){return Le(t).pipe(Gt(i=>BN(i.component,i.route,e,n)),mi(i=>i!==!0,!0))}function FN(t,n,e){return Le(n).pipe(Uo(i=>Bo(LN(i.route.parent,e),PN(i.route,e),jN(t,i.path),VN(t,i.route))),mi(i=>i!==!0,!0))}function PN(t,n){return t!==null&&n&&n(new Tu(t)),V(!0)}function LN(t,n){return t!==null&&n&&n(new Iu(t)),V(!0)}function VN(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return V(!0);let i=e.map(r=>Fr(()=>{let o=n._environmentInjector,s=xs(r,o),a=MN(s)?s.canActivate(n,t):ft(o,()=>s(n,t));return po(a).pipe(mi())}));return V(i).pipe(Es())}function jN(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>wN(o)).filter(o=>o!==null).map(o=>Fr(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=xs(a,c),d=TN(l)?l.canActivateChild(e,t):ft(c,()=>l(e,t));return po(d).pipe(mi())});return V(s).pipe(Es())}));return V(r).pipe(Es())}function BN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return V(!0);let o=r.map(s=>{let a=n._environmentInjector,c=xs(s,a),l=AN(c)?c.canDeactivate(t,n,e,i):ft(a,()=>c(t,n,e,i));return po(l).pipe(mi())});return V(o).pipe(Es())}function UN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return V(!0);let s=o.map(a=>{let c=xs(a,t),l=IN(c)?c.canLoad(n,e):ft(t,()=>c(n,e)),d=po(l);return r?d.pipe(uE(r)):d});return V(s).pipe(Es(),fE(i))}function fE(t){return Yf(Ze(n=>{if(typeof n!="boolean")throw ku(t,n)}),ne(n=>n===!0))}function HN(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return V(!0);let a=s.map(c=>{let l=xs(c,t),d=RN(l)?l.canMatch(n,e,r):ft(t,()=>l(n,e,r));return po(d).pipe(uE(o))});return V(a).pipe(Es(),fE(i))}var ki=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},dc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function zN(t){throw new x(4e3,!1)}function $N(t){throw aE(!1,Ot.GuardRejected)}var Dg=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}lineralizeSegments(n,e){return De(this,null,function*(){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[X])throw zN(`${n.redirectTo}`);r=r.children[X]}})}applyRedirectCommands(n,e,i,r,o){return De(this,null,function*(){let s=yield GN(e,r,o);if(s instanceof dn)throw new dc(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new dc(a);return a})}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new dn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new Ce(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new x(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function GN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return _u(po(ft(e,()=>i(n))))}function WN(t,n){return t.providers&&!t._injector&&(t._injector=Pa(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Zn(t){return t.outlet||X}function qN(t,n){let e=t.filter(i=>Zn(i)===n);return e.push(...t.filter(i=>Zn(i)!==n)),e}var xg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function hE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function YN(t,n,e,i,r,o,s){let a=pE(t,n,e);if(!a.matched)return V(a);let c=hE(o(a));return i=WN(n,i),HN(i,n,e,r,c,s).pipe(ne(l=>l===!0?a:C({},xg)))}function pE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},xg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||VC)(e,t,n);if(!r)return C({},xg);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function FC(t,n,e,i,r){return e.length>0&&QN(t,e,i,r)?{segmentGroup:new Ce(n,KN(i,new Ce(e,t.children))),slicedSegments:[]}:e.length===0&&XN(t,e,i)?{segmentGroup:new Ce(t.segments,ZN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Ce(t.segments,t.children),slicedSegments:e}}function ZN(t,n,e,i){let r={};for(let o of e)if(Lu(t,n,o)&&!i[Zn(o)]){let s=new Ce([],{});r[Zn(o)]=s}return C(C({},i),r)}function KN(t,n){let e={};e[X]=n;for(let i of t)if(i.path===""&&Zn(i)!==X){let r=new Ce([],{});e[Zn(i)]=r}return e}function QN(t,n,e,i){return e.some(r=>!Lu(t,n,r)||!(Zn(r)!==X)?!1:!(i!==void 0&&Zn(r)===i))}function XN(t,n,e){return e.some(i=>Lu(t,n,i))}function Lu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function JN(t,n,e){return n.length===0&&!t.children[e]}var Sg=class{};function ek(t,n,e,i,r,o,s,a){return De(this,null,function*(){return new Ig(t,n,e,i,r,s,o,a).recognize()})}var tk=31,Ig=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Dg(this.urlSerializer,this.urlTree)}noMatchError(n){return new x(4002,`'${n.segmentGroup}'`)}recognize(){return De(this,null,function*(){let n=FC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=yield this.match(n),r=new ln(i,e),o=new cc("",r),s=KC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}})}match(n){return De(this,null,function*(){let e=new bs([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),X,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,n,X,e),rootSnapshot:e}}catch(i){if(i instanceof dc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof ki?this.noMatchError(i):i}})}processSegmentGroup(n,e,i,r,o){return De(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=yield this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof ln?[s]:[]})}processChildren(n,e,i,r){return De(this,null,function*(){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=qN(e,c),f=yield this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=mE(s);return nk(a),a})}processSegment(n,e,i,r,o,s,a){return De(this,null,function*(){for(let c of e)try{return yield this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof ki||lE(l))continue;throw l}if(JN(i,r,o))return new Sg;throw new ki(i)})}processSegmentAgainstRoute(n,e,i,r,o,s,a,c){return De(this,null,function*(){if(Zn(i)!==s&&(s===X||!Lu(r,o,i)))throw new ki(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new ki(r)})}expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){return De(this,null,function*(){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:m}=pE(e,r,o);if(!c)throw new ki(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>tk&&(this.allowRedirects=!1));let g=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=yield this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,hE(g),n),S=yield this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,S.concat(m),s,!1,a)})}createSnapshot(n,e,i,r,o){let s=new bs(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,rk(e),Zn(e),e.component??e._loadedComponent??null,e,ok(e),n),a=Mg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}matchSegmentAgainstRoute(n,e,i,r,o,s){return De(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Tt=>this.createSnapshot(n,i,Tt.consumedSegments,Tt.parameters,s),c=yield _u(YN(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new ki(e);n=i._injector??n;let{routes:l}=yield this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:m,remainingSegments:g}=c,b=this.createSnapshot(n,i,m,f,s),{segmentGroup:S,slicedSegments:N}=FC(e,m,g,l,o);if(N.length===0&&S.hasChildren()){let Tt=yield this.processChildren(d,l,S,b);return new ln(b,Tt)}if(l.length===0&&N.length===0)return new ln(b,[]);let ie=Zn(i)===o,st=yield this.processSegment(d,l,S,N,ie?X:o,!0,b);return new ln(b,st instanceof ln?[st]:[])})}getChildConfig(n,e,i){return De(this,null,function*(){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield _u(UN(n,e,i,this.urlSerializer,this.abortSignal))){let o=yield this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw $N(e)}return{routes:[],injector:n}})}};function nk(t){t.sort((n,e)=>n.value.outlet===X?-1:e.value.outlet===X?1:n.value.outlet.localeCompare(e.value.outlet))}function ik(t){let n=t.value.routeConfig;return n&&n.path===""}function mE(t){let n=[],e=new Set;for(let i of t){if(!ik(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=mE(i.children);n.push(new ln(i.value,r))}return n.filter(i=>!e.has(i))}function rk(t){return t.data||{}}function ok(t){return t.resolve||{}}function sk(t,n,e,i,r,o,s){return Gt(a=>De(null,null,function*(){let{state:c,tree:l}=yield ek(t,n,e,i,a.extractedUrl,r,o,s);return z(C({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function ak(t){return Gt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return V(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of gE(a))o.add(c);let s=0;return Le(o).pipe(Uo(a=>r.has(a)?ck(a,e,t):(a.data=Mg(a,a.parent,t).resolve,V(void 0))),Ze(()=>s++),Ml(1),Gt(a=>s===o.size?V(n):Ye))})}function gE(t){let n=t.children.map(e=>gE(e)).flat();return[t,...n]}function ck(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!rE(i)&&(r[uc]=i.title),Fr(()=>(t.data=Mg(t,t.parent,e).resolve,lk(r,t,n).pipe(ne(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function lk(t,n,e){let i=pg(t);if(i.length===0)return V({});let r={};return Le(i).pipe(Gt(o=>dk(t[o],n,e).pipe(mi(),Ze(s=>{if(s instanceof Cs)throw ku(new ar,s);r[o]=s}))),Ml(1),ne(()=>r),Pr(o=>lE(o)?Ye:ta(o)))}function dk(t,n,e){let i=n._environmentInjector,r=xs(t,i),o=r.resolve?r.resolve(n,e):ft(i,()=>r(n,e));return po(o)}function PC(t){return We(n=>{let e=t(n);return e?Le(e).pipe(ne(()=>n)):V(n)})}var Ng=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===X);return i}getResolvedTitleForRoute(e){return e.data[uc]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(vE)})}return t})(),vE=(()=>{class t extends Ng{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(M(MC))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ss=new y("",{factory:()=>({})}),pc=new y(""),_E=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(km);loadComponent(e,i){return De(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=De(this,null,function*(){try{let o=yield BC(ft(e,()=>i.loadComponent())),s=yield bE(Hm(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=De(this,null,function*(){try{let o=yield yE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function yE(t,n,e,i){return De(this,null,function*(){let r=yield BC(ft(e,()=>t.loadChildren())),o=yield bE(Hm(r)),s;o instanceof Ud||Array.isArray(o)?s=o:s=yield n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,d=s,c=a.get(pc,[],{optional:!0,self:!0}).flat()),{routes:c.map(Rg),injector:a,factory:d}})}function bE(t){return De(this,null,function*(){return t})}var Vu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(uk)})}return t})(),uk=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),CE=new y("");var EE=new y(""),fk=()=>{},wE=new y(""),DE=(()=>{class t{currentNavigation=ee(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ee(null);events=new I;transitionAbortWithErrorSubject=new I;configLoader=u(_E);environmentInjector=u(xe);destroyRef=u(wt);urlSerializer=u(ws);rootContexts=u(Ds);location=u(or);inputBindingEnabled=u(Pu,{optional:!0})!==null;titleStrategy=u(Ng);options=u(Ss,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||vN;urlHandlingStrategy=u(Vu);createViewTransition=u(CE,{optional:!0});navigationErrorHandler=u(wE,{optional:!0});activatedRouteInjectorFeature=u(EE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>V(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new xu(r)),i=r=>this.events.next(new Su(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;ge(()=>{this.transitions?.next(z(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new at(null),this.transitions.pipe(Ie(i=>i!==null),We(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return V(i).pipe(We(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Ot.SupersededByNewNavigation),Ye;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?z(C({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new Oi(c.id,this.urlSerializer.serialize(c.rawUrl),"",rc.IgnoredSameUrlNavigation)),c.resolve(!1),Ye;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return V(c).pipe(We(m=>(this.events.next(new fo(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?Ye:Promise.resolve(m))),sk(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),Ze(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=m.urlAfterRedirects,g)),this.events.next(new sc)}),We(m=>Le(i.routesRecognizeHandler.deferredHandle??V(void 0)).pipe(ne(()=>m))),Ze(()=>{let m=new oc(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:m,extractedUrl:g,source:b,restoredState:S,extras:N}=c,ie=new fo(m,this.urlSerializer.serialize(g),b,S);this.events.next(ie);let st=nE(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=z(C({},c),{targetSnapshot:st,urlAfterRedirects:g,extras:z(C({},N),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Tt=>(Tt.finalUrl=g,Tt)),V(i)}else return this.events.next(new Oi(c.id,this.urlSerializer.serialize(c.extractedUrl),"",rc.IgnoredByUrlHandlingStrategy)),c.resolve(!1),Ye}),ne(c=>{let l=new Cu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=z(C({},c),{guards:EN(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),kN(c=>this.events.next(c)),We(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw ku(this.urlSerializer,c.guardsResult);let l=new Eu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return Ye;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",Ot.GuardRejected),Ye;if(c.guards.canActivateChecks.length===0)return V(c);let d=new wu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!a())return Ye;let f=!1;return V(c).pipe(ak(this.paramsInheritanceStrategy),Ze({next:()=>{f=!0;let m=new Du(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)},complete:()=>{f||this.cancelNavigationTransition(c,"",Ot.NoDataFromResolver)}}))}),PC(c=>{let l=f=>{let m=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let g=f._environmentInjector;m.push(this.configLoader.loadComponent(g,f.routeConfig).then(b=>{f.component=b}))}for(let g of f.children)m.push(...l(g));return m},d=l(c.targetSnapshot.root);return d.length===0?V(c):Le(Promise.all(d).then(()=>c))}),We(c=>{let{newlyCreatedRoutes:l,state:d}=_N(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=z(C({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),V(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),PC(()=>this.afterPreactivation()),We(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?Le(d).pipe(ne(()=>i)):V(i)}),Wt(1),We(c=>{r=!1,this.events.next(new _s);let l=i.beforeActivateHandler.deferredHandle;return l?Le(l.then(()=>c)):V(c)}),Ze(c=>{new wg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=fk,l)),this.lastSuccessfulNavigation.set(ge(this.currentNavigation)),this.events.next(new ci(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),ke(dE(s.signal).pipe(Ie(()=>!o&&r),Ze(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",Ot.Aborted)}))),Ze({complete:()=>{o=!0}}),ke(this.transitionAbortWithErrorSubject.pipe(Ze(c=>{throw c}))),Lr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",Ot.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Pr(c=>{if(o=!0,LC(i),this.destroyed)return i.resolve(!1),Ye;if(cE(c))this.events.next(new An(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),CN(c)?this.events.next(new ys(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new ho(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=ft(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof Cs){let{message:f,cancellationCode:m}=ku(this.urlSerializer,d);this.events.next(new An(i.id,this.urlSerializer.serialize(i.extractedUrl),f,m)),this.events.next(new ys(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return Ye}))}))}cancelNavigationTransition(e,i,r){LC(e);let o=new An(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=ge(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function hk(t){return t!==tc}function LC(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var xE=new y("");var SE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(pk)})}return t})(),Fu=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},pk=(()=>{class t extends Fu{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),ju=(()=>{class t{urlSerializer=u(ws);options=u(Ss,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(or);urlHandlingStrategy=u(Vu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new dn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof dn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=nE(null,u(xe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(mk)})}return t})(),mk=(()=>{class t extends ju{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof fo?this.updateStateMemento():e instanceof Oi?this.commitTransition(i):e instanceof oc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof _s?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof An&&!tE(e)?this.restoreHistory(i):e instanceof ho?this.restoreHistory(i,!0):e instanceof ci&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=C(C({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=C(C({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function kg(t,n){t.events.pipe(Ie(e=>e instanceof ci||e instanceof An||e instanceof ho||e instanceof Oi),ne(e=>e instanceof ci||e instanceof Oi?0:(e instanceof An?e.code===Ot.Redirect||e.code===Ot.SupersededByNewNavigation:!1)?2:1),Ie(e=>e!==2),Wt(1)).subscribe(()=>{n()})}var Ve=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u($d);stateManager=u(ju);options=u(Ss,{optional:!0})||{};pendingTasks=u(Di);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(DE);urlSerializer=u(ws);location=u(or);urlHandlingStrategy=u(Vu);injector=u(xe);_events=new I;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(SE);injectorCleanup=u(xE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(pc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Pu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new fe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=ge(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof An&&i.code!==Ot.Redirect&&i.code!==Ot.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ci)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof ys){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||hk(r.source)},s);this.scheduleNavigation(a,tc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}mN(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),tc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=z(C({},o),{browserUrl:e})),r){let l=C({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(an)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return ge(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Rg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let m=r?r.snapshot:this.routerState.snapshot.root;f=QC(m)}catch(m){(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return XC(f,e,d,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=cr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,tc,null,i)}navigate(e,i={skipLocationChange:!1}){return gk(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch(i){return this.console.warn(_i(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},HC):i===!1?r=C({},mg):r=C(C({},mg),i),cr(e))return AC(this.currentUrlTree,e,r);let o=this.parseUrl(e);return AC(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,m)=>{a=f,c=m});let d=this.pendingTasks.add();return kg(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function gk(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new x(4008,!1)}var yk=(()=>{class t{router=u(Ve);stateManager=u(ju);fragment=ee("");queryParams=ee({});path=ee("");serializer=u(ws);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof ci&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new dn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),rt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new ir("href"),{optional:!0});reactiveHref=Om(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return ge(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return ge(this._target)}_target=ee(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return ge(this._queryParams)}_queryParams=ee(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return ge(this._fragment)}_fragment=ee(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return ge(this._queryParamsHandling)}_queryParamsHandling=ee(void 0);set state(e){this._state.set(e)}get state(){return ge(this._state)}_state=ee(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return ge(this._info)}_info=ee(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return ge(this._relativeTo)}_relativeTo=ee(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return ge(this._preserveFragment)}_preserveFragment=ee(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return ge(this._skipLocationChange)}_skipLocationChange=ee(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return ge(this._replaceUrl)}_replaceUrl=ee(!1);browserUrl=ls(void 0);isAnchorElement;onChanges=new I;applicationErrorHandler=u(an);options=u(Ss,{optional:!0});reactiveRouterState=u(yk);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=ee(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(cr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=C({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Yt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:cr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return ge(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(w(Ve),w(Zt),Ra("tabindex"),w(je),w(j),w(us))};static \u0275dir=k({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&F("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&he("href",r.reactiveHref(),Xp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",B],skipLocationChange:[2,"skipLocationChange","skipLocationChange",B],replaceUrl:[2,"replaceUrl","replaceUrl",B],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[nt]})}return t})();var bk=new y("");function Og(t,...n){return yi([{provide:pc,multi:!0,useValue:t},{provide:Zt,useFactory:Ck},{provide:La,multi:!0,useFactory:Ek},n.map(e=>e.\u0275providers)])}function Ck(){return u(Ve).routerState.root}function Ek(){let t=u(ae);return n=>{let e=t.get(tn);if(n!==e.components[0])return;let i=t.get(Ve),r=t.get(wk);t.get(Dk)===1&&i.initialNavigation(),t.get(xk,null,{optional:!0})?.setUpPreloading(),t.get(bk,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var wk=new y("",{factory:()=>new I}),Dk=new y("",{factory:()=>1});var xk=new y("");var FE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(w(je),w(j))};static \u0275dir=k({type:t})}return t})(),Sk=(()=>{class t extends FE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=qn(t)))(r||t)}})();static \u0275dir=k({type:t,features:[Be]})}return t})(),PE=new y("");var Ik={provide:PE,useExisting:pn(()=>mt),multi:!0};function Mk(){let t=Mn()?Mn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var Tk=new y(""),mt=(()=>{class t extends FE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!Mk())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(w(je),w(j),w(Tk,8))};static \u0275dir=k({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&F("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[ze([Ik]),Be]})}return t})();function Lg(t){return t==null||Vg(t)===0}function Vg(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Qu=new y(""),jg=new y(""),Ak=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,go=class{static min(n){return Rk(n)}static max(n){return Nk(n)}static required(n){return LE(n)}static requiredTrue(n){return kk(n)}static email(n){return Ok(n)}static minLength(n){return Fk(n)}static maxLength(n){return Pk(n)}static pattern(n){return Lk(n)}static nullValidator(n){return Uu()}static compose(n){return zE(n)}static composeAsync(n){return $E(n)}};function Rk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function Nk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function LE(t){return Lg(t.value)?{required:!0}:null}function kk(t){return t.value===!0?null:{required:!0}}function Ok(t){return Lg(t.value)||Ak.test(t.value)?null:{email:!0}}function Fk(t){return n=>{let e=n.value?.length??Vg(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function Pk(t){return n=>{let e=n.value?.length??Vg(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function Lk(t){if(!t)return Uu;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Lg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Uu(t){return null}function VE(t){return t!=null}function jE(t){return nr(t)?Le(t):t}function BE(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function UE(t,n){return n.map(e=>e(t))}function Vk(t){return!t.validate}function HE(t){return t.map(n=>Vk(n)?n:e=>n.validate(e))}function zE(t){if(!t)return null;let n=t.filter(VE);return n.length==0?null:function(e){return BE(UE(e,n))}}function Bg(t){return t!=null?zE(HE(t)):null}function $E(t){if(!t)return null;let n=t.filter(VE);return n.length==0?null:function(e){let i=UE(e,n).map(jE);return ia(i).pipe(ne(BE))}}function Ug(t){return t!=null?$E(HE(t)):null}function IE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function GE(t){return t._rawValidators}function WE(t){return t._rawAsyncValidators}function Fg(t){return t?Array.isArray(t)?t:[t]:[]}function Hu(t,n){return Array.isArray(t)?t.includes(n):t===n}function ME(t,n){let e=Fg(n);return Fg(t).forEach(r=>{Hu(e,r)||e.push(r)}),e}function TE(t,n){return Fg(n).filter(e=>!Hu(t,e))}var zu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Bg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Ug(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},lr=class extends zu{name;get formDirective(){return null}get path(){return null}};var mc="VALID",Bu="INVALID",Is="PENDING",gc="DISABLED",dr=class{},$u=class extends dr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},_c=class extends dr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},yc=class extends dr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Ms=class extends dr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Gu=class extends dr{source;constructor(n){super(),this.source=n}},Ts=class extends dr{source;constructor(n){super(),this.source=n}};function qE(t){return(Xu(t)?t.validators:t)||null}function jk(t){return Array.isArray(t)?Bg(t):t||null}function YE(t,n){return(Xu(n)?n.asyncValidators:t)||null}function Bk(t){return Array.isArray(t)?Ug(t):t||null}function Xu(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function Uk(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new x(1e3,"");if(!ZE(i,e))throw new x(1001,"")}function Hk(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new x(-1002,"")})}var Wu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=ee(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return ge(this.statusReactive)}set status(n){ge(()=>this.statusReactive.set(n))}_status=Yt(()=>this.statusReactive());statusReactive=ee(void 0);get valid(){return this.status===mc}get invalid(){return this.status===Bu}get pending(){return this.status===Is}get disabled(){return this.status===gc}get enabled(){return this.status!==gc}errors;get pristine(){return ge(this.pristineReactive)}set pristine(n){ge(()=>this.pristineReactive.set(n))}_pristine=Yt(()=>this.pristineReactive());pristineReactive=ee(!0);get dirty(){return!this.pristine}get touched(){return ge(this.touchedReactive)}set touched(n){ge(()=>this.touchedReactive.set(n))}_touched=Yt(()=>this.touchedReactive());touchedReactive=ee(!1);get untouched(){return!this.touched}_events=new I;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(ME(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(ME(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(TE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(TE(n,this._rawAsyncValidators))}hasValidator(n){return Hu(this._rawValidators,n)}hasAsyncValidator(n){return Hu(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(z(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new yc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new yc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(z(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new _c(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new _c(!0,i))}markAsPending(n={}){this.status=Is;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Ms(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(z(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=gc,this.errors=null,this._forEachChild(r=>{r.disable(z(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new $u(this.value,i)),this._events.next(new Ms(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(z(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=mc,this._forEachChild(i=>{i.enable(z(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(z(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===mc||this.status===Is)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new $u(this.value,e)),this._events.next(new Ms(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(z(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?gc:mc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Is,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=jE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Ms(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new Y,this.statusChanges=new Y}_calculateStatus(){return this._allControlsDisabled()?gc:this.errors?Bu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Is)?Is:this._anyControlsHaveStatus(Bu)?Bu:mc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new _c(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new yc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Xu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=jk(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=Bk(this._rawAsyncValidators)}_updateHasRequiredValidator(){ge(()=>this._hasRequired.set(this.hasValidator(go.required)))}};function ZE(t,n){return Object.hasOwn(t,n)}function zk(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function $k(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Pg=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var Gk=(()=>{class t{_validator=Uu;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Uu,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,features:[nt]})}return t})();var Wk={provide:Qu,useExisting:pn(()=>Rn),multi:!0};var Rn=(()=>{class t extends Gk{required;inputName="required";normalizeInput=B;createValidator=e=>LE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&he("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[ze([Wk]),Be]})}return t})();var qk=new y(""),Ju=new y("",{factory:()=>Hg}),Hg="always";function Yk(t,n){return[...n.path,t]}function AE(t,n,e=Hg){zg(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),Kk(t,n),Xk(t,n),Qk(t,n),Zk(t,n)}function RE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Yu(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function qu(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function Zk(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function zg(t,n){let e=GE(t);n.validator!==null?t.setValidators(IE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=WE(t);n.asyncValidator!==null?t.setAsyncValidators(IE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();qu(n._rawValidators,r),qu(n._rawAsyncValidators,r)}function Yu(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=GE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=WE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return qu(n._rawValidators,i),qu(n._rawAsyncValidators,i),e}function Kk(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&KE(t,n)})}function Qk(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&KE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function KE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function Xk(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function QE(t,n){t==null,zg(t,n)}function Jk(t,n){return Yu(t,n)}function eO(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function tO(t){return Object.getPrototypeOf(t.constructor)===Sk}function XE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function nO(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===mt?e=o:tO(o)?i=o:r=o}),r||i||e||null}function iO(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var rO={provide:qk,useFactory:()=>{let t=u(li,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},li=class extends zu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Ts&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=nO(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(wt)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(me);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new fe,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Ts&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=zk(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof Rn))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&$k(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Pg({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=Yt(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Hn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},Zu=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Ft=(()=>{class t extends Zu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(w(li,2))};static \u0275dir=k({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ce("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Be]})}return t})(),di=(()=>{class t extends Zu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(w(lr,10))};static \u0275dir=k({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ce("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Be]})}return t})(),Ku=class extends Wu{constructor(n,e,i){super(qE(e),YE(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){ge(()=>{Hk(this,!0,n),Object.keys(n).forEach(i=>{Uk(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,z(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ts(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return ZE(this.controls,n)?this.controls[n]:null}};var oO={provide:lr,useExisting:pn(()=>Kt)},vc=Promise.resolve(),Kt=(()=>{class t extends lr{callSetDisabledState;get submitted(){return ge(this.submittedReactive)}_submitted=Yt(()=>this.submittedReactive());submittedReactive=ee(!1);_directives=new Set;form;ngSubmit=new Y;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Ku({},Bg(e),Ug(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){vc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){vc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){vc.then(()=>{let i=this._findContainer(e.path),r=new Ku({});QE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){vc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){vc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),XE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Gu(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(w(Qu,10),w(jg,10),w(Ju,8))};static \u0275dir=k({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&F("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ze([oO]),Be]})}return t})();function NE(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function kE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var JE=class extends Wu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(qE(e),YE(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Xu(e)&&(e.nonNullable||e.initialValueIsDefault)&&(kE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){ge(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Ts(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){NE(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){NE(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){kE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var sO=t=>t instanceof JE;var aO={provide:li,useExisting:pn(()=>xt)},OE=Promise.resolve(),xt=(()=>{class t extends li{_changeDetectorRef;callSetDisabledState;control=new JE;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Y;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),eO(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,AE(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,AE(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){OE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&B(i);OE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?Yk(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(w(lr,9),w(Qu,10),w(jg,10),w(PE,10),w(me,8),w(Ju,8),w(ae,8),w(je,8))};static \u0275dir=k({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[ze([aO,rO]),Be,nt,Mm(null)]})}return t})();var ui=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var cO=(()=>{class t extends lr{callSetDisabledState;get submitted(){return ge(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Yt(()=>this._submittedReactive());_submittedReactive=ee(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Yu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){RE(e.control||null,e,!1),iO(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,XE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Gu(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(RE(i||null,e),sO(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);QE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&Jk(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){zg(this.form,this),this._oldForm&&Yu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(w(Qu,10),w(jg,10),w(Ju,8))};static \u0275dir=k({type:t,features:[Be,nt]})}return t})();var lO={provide:lr,useExisting:pn(()=>bc)},bc=(()=>{class t extends cO{form=null;ngSubmit=new Y;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&F("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ze([lO]),Be]})}return t})();var dO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})();var Pt=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Ju,useValue:e.callSetDisabledState??Hg}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[dO]})}return t})();var uO=new y("cdk-dir-doc",{providedIn:"root",factory:()=>u(U)}),fO=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function ew(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?fO.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Nn=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ee("ltr");change=new Y;constructor(){let e=u(uO,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(ew(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Se=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})();var hO=["*"];var pO=new y("MAT_CARD_CONFIG"),Lt=(()=>{class t{appearance;constructor(){let e=u(pO,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ce("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:hO,decls:1,vars:0,template:function(i,r){i&1&&(He(),ue(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2})}return t})();var Vt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se]})}return t})();function vo(t){return t.buttons===0||t.detail===0}function _o(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var $g;function tw(){if($g==null){let t=typeof document<"u"?document.head:null;$g=!!(t&&(t.createShadowRoot||t.attachShadow))}return $g}function Gg(t){if(tw()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function St(t){if(t.composedPath)try{return t.composedPath()[0]}catch(n){}return t.target}var Wg;try{Wg=typeof Intl<"u"&&Intl.v8BreakIterator}catch(t){Wg=!1}var $e=(()=>{class t{_platformId=u(Xr);isBrowser=this._platformId?cC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Wg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Cc;function nw(){if(Cc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Cc=!0}))}finally{Cc=Cc||!1}return Cc}function As(t){return nw()?t:!!t.capture}function fn(t){return t instanceof j?t.nativeElement:t}var iw=new y("cdk-input-modality-detector-options"),rw={ignoreKeys:[18,17,224,91,16]},ow=650,qg={passive:!0,capture:!0},sw=(()=>{class t{_platform=u($e);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new at(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=St(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<ow||(this._modality.next(vo(e)?"keyboard":"mouse"),this._mostRecentTarget=St(e))};_onTouchstart=e=>{if(_o(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=St(e)};constructor(){let e=u(O),i=u(U),r=u(iw,{optional:!0});if(this._options=C(C({},rw),r),this.modalityDetected=this._modality.pipe(th(1)),this.modalityChanged=this.modalityDetected.pipe(Il()),this._platform.isBrowser){let o=u(tt).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,qg),o.listen(i,"mousedown",this._onMousedown,qg),o.listen(i,"touchstart",this._onTouchstart,qg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Ec=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Ec||{}),aw=new y("cdk-focus-monitor-default-options"),ef=As({passive:!0,capture:!0}),hr=(()=>{class t{_ngZone=u(O);_platform=u($e);_inputModalityDetector=u(sw);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(U);_stopInputModalityDetector=new I;constructor(){let e=u(aw,{optional:!0});this._detectionMode=e?.detectionMode||Ec.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=St(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=fn(e);if(!this._platform.isBrowser||r.nodeType!==1)return V();let o=Gg(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new I,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=fn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=fn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Ec.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Ec.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?ow:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=St(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,ef),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,ef)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(ke(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,ef),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,ef),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var tf=new WeakMap,It=(()=>{class t{_appRef;_injector=u(ae);_environmentInjector=u(xe);load(e){let i=this._appRef=this._appRef||this._injector.get(tn),r=tf.get(i);r||(r={loaders:new Set,refs:[]},tf.set(i,r),i.onDestroy(()=>{tf.get(i)?.refs.forEach(o=>o.destroy()),tf.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Jd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var wc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2})}return t})(),nf;function mO(){if(nf===void 0&&(nf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(nf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return nf}function yo(t){return mO()?.createHTML(t)||t}function cw(t,n,e){let i=e.sanitize(ct.HTML,n);t.innerHTML=yo(i||"")}function Yg(t){return Array.isArray(t)?t:[t]}var lw=new Set,bo,Zg=(()=>{class t{_platform=u($e);_nonce=u(Jr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):vO}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&gO(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function gO(t,n){if(!lw.has(t))try{bo||(bo=document.createElement("style"),n&&bo.setAttribute("nonce",n),bo.setAttribute("type","text/css"),document.head.appendChild(bo)),bo.sheet&&(bo.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),lw.add(t))}catch(e){console.error(e)}}function vO(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var _O=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var dw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({providers:[_O]})}return t})();var uw=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),fw=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),yO=0,Kg=(()=>{class t{_ngZone=u(O);_defaultOptions=u(fw,{optional:!0});_liveElement;_document=u(U);_sanitizer=u(Qa);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(uw,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:cw(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${yO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var bO=200,rf=class{_letterKeyStream=new I;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new I;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:bO;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Ze(e=>this._pressedLetters.push(e)),Jf(n),Ie(()=>this._pressedLetters.length>0),ne(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function kn(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Rs=class{_items;_activeItemIndex=ee(-1);_activeItem=ee(null);_wrap=!1;_typeaheadSubscription=fe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Gn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):bn(n)&&(this._effectRef=Hn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new I;change=new I;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new rf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||kn(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return bn(this._items)?this._items():this._items instanceof Gn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Mc=class extends Rs{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Co=class extends Rs{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var pw=new Map,gt=class t{_appId=u(Qr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=pw.get(n);return i===void 0?i=0:i++,pw.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})};var Eo;function mw(){if(Eo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Eo=!1,Eo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Eo=!0;else{let t=Element.prototype.scrollTo;t?Eo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Eo=!1}}return Eo}function Qg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Ns,gw=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Xg(){if(Ns)return Ns;if(typeof document!="object"||!document)return Ns=new Set(gw),Ns;let t=document.createElement("input");return Ns=new Set(gw.filter(n=>(t.setAttribute("type",n),t.type===n))),Ns}var CO=new y("MATERIAL_ANIMATIONS"),vw=null;function Jg(){return u(CO,{optional:!0})?.animationsDisabled||u(ya,{optional:!0})==="NoopAnimations"?"di-disabled":(vw??=u(Zg).matchMedia("(prefers-reduced-motion)").matches,vw?"reduced-motion":"enabled")}function Mt(){return Jg()!=="enabled"}function Je(t){return t==null?"":typeof t=="string"?t:`${t}px`}function ks(t){return t!=null&&`${t}`!="false"}var On=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(On||{}),ev=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=On.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},yw=As({passive:!0,capture:!0}),tv=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,yw)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,yw)))}_delegateEventHandler=n=>{let e=St(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Tc={enterDuration:225,exitDuration:150},EO=800,bw=As({passive:!0,capture:!0}),Cw=["mousedown","touchstart"],Ew=["mouseup","mouseleave","touchend","touchcancel"],wO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2})}return t})(),Ac=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new tv;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=fn(i)),o&&o.get(It).load(wO)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},Tc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||DO(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),m=f.transitionProperty,g=f.transitionDuration,b=m==="none"||g==="0s"||g==="0s, 0s"||r.width===0&&r.height===0,S=new ev(this,d,i,b);d.style.transform="scale3d(1, 1, 1)",S.state=On.FADING_IN,i.persistent||(this._mostRecentTransientRipple=S);let N=null;return!b&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ie=()=>{N&&(N.fallbackTimer=null),clearTimeout(Tt),this._finishRippleTransition(S)},st=()=>this._destroyRipple(S),Tt=setTimeout(st,l+100);d.addEventListener("transitionend",ie),d.addEventListener("transitioncancel",st),N={onTransitionEnd:ie,onTransitionCancel:st,fallbackTimer:Tt}}),this._activeRipples.set(S,N),(b||!l)&&this._finishRippleTransition(S),S}fadeOutRipple(n){if(n.state===On.FADING_OUT||n.state===On.HIDDEN)return;let e=n.element,i=C(C({},Tc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=On.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=fn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,Cw.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Ew.forEach(e=>{this._triggerElement.addEventListener(e,this,bw)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===On.FADING_IN?this._startFadeOutTransition(n):n.state===On.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=On.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=On.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=vo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+EO;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!_o(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===On.VISIBLE||n.config.terminateOnPointerUp&&n.state===On.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(Cw.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(Ew.forEach(e=>n.removeEventListener(e,this,bw)),this._pointerUpEventsRegistered=!1))}};function DO(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Rc=new y("mat-ripple-global-options"),of=(()=>{class t{_elementRef=u(j);_animationsDisabled=Mt();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(O),i=u($e),r=u(Rc,{optional:!0}),o=u(ae);this._globalOptions=r||{},this._rippleRenderer=new Ac(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var xO={capture:!0},SO=["focus","mousedown","mouseenter","touchstart"],nv="mat-ripple-loader-uninitialized",iv="mat-ripple-loader-class-name",ww="mat-ripple-loader-centered",sf="mat-ripple-loader-disabled",af=(()=>{class t{_document=u(U);_animationsDisabled=Mt();_globalRippleOptions=u(Rc,{optional:!0});_platform=u($e);_ngZone=u(O);_injector=u(ae);_eventCleanups;_hosts=new Map;constructor(){let e=u(tt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>SO.map(i=>e.listen(this._document,i,this._onInteraction,xO)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(nv,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(iv))&&e.setAttribute(iv,i.className||""),i.centered&&e.setAttribute(ww,""),i.disabled&&e.setAttribute(sf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(sf,""):e.removeAttribute(sf)}_onInteraction=e=>{let i=St(e);if(i instanceof HTMLElement){let r=i.closest(`[${nv}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(iv)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Tc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Tc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(sf),rippleConfig:{centered:e.hasAttribute(ww),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Ac(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(nv)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Fi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2})}return t})();var IO=["*",[["","progressIndicator",""]]],MO=["*","[progressIndicator]"];function TO(t,n){t&1&&(ht(0,"div",1),ue(1,1),Dt())}var AO=new y("MAT_BUTTON_CONFIG");function Dw(t){return t==null?void 0:In(t)}var rv=(()=>{class t{_elementRef=u(j);_ngZone=u(O);_animationsDisabled=Mt();_config=u(AO,{optional:!0});_focusMonitor=u(hr);_cleanupClick;_renderer=u(je);_rippleLoader=u(af);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=ls(!1,{transform:B});constructor(){u(It).load(Fi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(he("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),cn(r.color?"mat-"+r.color:""),ce("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B],ariaDisabled:[2,"aria-disabled","ariaDisabled",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B],tabIndex:[2,"tabIndex","tabIndex",Dw],_tabindex:[2,"tabindex","_tabindex",Dw],showProgress:[1,"showProgress"]}})}return t})(),Pi=(()=>{class t extends rv{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:MO,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(He(IO),wn(0,"span",0),ue(1),_e(2,TO,2,0,"div",1),wn(3,"span",2)(4,"span",3)),i&2&&(_(2),ye(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2})}return t})();var pr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se]})}return t})();var RO=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],NO=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function kO(t,n){t&1&&(ht(0,"div",2),ue(1,3),Dt())}var xw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),jt=(()=>{class t extends rv{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=OO(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?xw.get(this._appearance):null,o=xw.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:NO,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(He(RO),wn(0,"span",0),ue(1),ht(2,"span",1),ue(3,1),Dt(),ue(4,2),_e(5,kO,2,0,"div",2),wn(6,"span",3)(7,"span",4)),i&2&&(ce("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),_(5),ye(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2})}return t})();function OO(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var vt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[pr,Se]})}return t})();var FO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2})}return t})(),PO={passive:!0},Sw=(()=>{class t{_platform=u($e);_ngZone=u(O);_renderer=u(tt).createRenderer(null,null);_styleLoader=u(It);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Ye;this._styleLoader.load(FO);let i=fn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new I,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,PO)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=fn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Iw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})();var Mw=new y("MAT_INPUT_VALUE_ACCESSOR");var ov=class{_box;_destroyed=new I;_resizeSubject=new I;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new re(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ie(e=>e.some(i=>i.target===n)),Al({bufferSize:1,refCount:!0}),ke(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Tw=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(O);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new ov(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var LO=["notch"],VO=["*"],Aw=["iconPrefixContainer"],Rw=["textPrefixContainer"],Nw=["iconSuffixContainer"],kw=["textSuffixContainer"],jO=["textField"],BO=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],UO=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function HO(t,n){t&1&&pe(0,"span",21)}function zO(t,n){if(t&1&&(p(0,"label",20),ue(1,1),_e(2,HO,1,0,"span",21),h()),t&2){let e=D(2);E("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),he("for",e._control.disableAutomaticLabeling?null:e._control.id),_(2),ye(!e.hideRequiredMarker&&e._control.required?2:-1)}}function $O(t,n){if(t&1&&_e(0,zO,3,5,"label",20),t&2){let e=D();ye(e._hasFloatingLabel()?0:-1)}}function GO(t,n){t&1&&pe(0,"div",7)}function WO(t,n){}function qO(t,n){if(t&1&&H(0,WO,0,0,"ng-template",13),t&2){D(2);let e=Sn(1);E("ngTemplateOutlet",e)}}function YO(t,n){if(t&1&&(p(0,"div",9),_e(1,qO,1,1,null,13),h()),t&2){let e=D();E("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),_(),ye(e._forceDisplayInfixLabel()?-1:1)}}function ZO(t,n){t&1&&(p(0,"div",10,2),ue(2,2),h())}function KO(t,n){t&1&&(p(0,"div",11,3),ue(2,3),h())}function QO(t,n){}function XO(t,n){if(t&1&&H(0,QO,0,0,"ng-template",13),t&2){D();let e=Sn(1);E("ngTemplateOutlet",e)}}function JO(t,n){t&1&&(p(0,"div",14,4),ue(2,4),h())}function e1(t,n){t&1&&(p(0,"div",15,5),ue(2,5),h())}function t1(t,n){t&1&&pe(0,"div",16)}function n1(t,n){t&1&&(p(0,"div",18),ue(1,6),h())}function i1(t,n){if(t&1&&(p(0,"mat-hint",22),v(1),h()),t&2){let e=D(2);E("id",e._hintLabelId),_(),Xe(e.hintLabel)}}function r1(t,n){if(t&1&&(p(0,"div",19),_e(1,i1,2,2,"mat-hint",22),ue(2,7),pe(3,"div",23),ue(4,8),h()),t&2){let e=D();_(),ye(e.hintLabel?1:-1)}}var dt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["mat-label"]]})}return t})(),o1=new y("MatError");var Nc=(()=>{class t{align="start";id=u(gt).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Dn("id",r.id),he("align",null),ce("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Bw=new y("MatPrefix"),wo=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matPrefix",""],["","matIconPrefix",""],["","matTextPrefix",""]],inputs:{_isTextSelector:[0,"matTextPrefix","_isTextSelector"]},features:[ze([{provide:Bw,useExisting:t}])]})}return t})(),Uw=new y("MatSuffix"),hn=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[ze([{provide:Uw,useExisting:t}])]})}return t})(),Hw=new y("FloatingLabelParent"),Ow=(()=>{class t{_elementRef=u(j);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(Tw);_ngZone=u(O);_parent=u(Hw);_resizeSubscription=new fe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return s1(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function s1(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Fw="mdc-line-ripple--active",cf="mdc-line-ripple--deactivating",Pw=(()=>{class t{_elementRef=u(j);_cleanupTransitionEnd;constructor(){let e=u(O),i=u(je);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(cf),e.add(Fw)}deactivate(){this._elementRef.nativeElement.classList.add(cf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(cf);e.propertyName==="opacity"&&r&&i.remove(Fw,cf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),Lw=(()=>{class t{_elementRef=u(j);_ngZone=u(O);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&kt(LO,5),i&2){let o;K(o=Q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:VO,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(He(),wn(0,"div",1),ht(1,"div",2,0),ue(3),Dt(),wn(4,"div",3))},encapsulation:2})}return t})(),kc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t})}return t})();var Oc=new y("MatFormField"),a1=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Vw="fill",c1="auto",jw="fixed",l1="translateY(-50%)",_t=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_platform=u($e);_idGenerator=u(gt);_ngZone=u(O);_defaults=u(a1,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Ua("iconPrefixContainer");_textPrefixContainerSignal=Ua("textPrefixContainer");_iconSuffixContainerSignal=Ua("iconSuffixContainer");_textSuffixContainerSignal=Ua("textSuffixContainer");_prefixSuffixContainers=Yt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Q0(dt);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=ks(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||c1}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||Vw;this._appearanceSignal.set(i)}_appearanceSignal=ee(Vw);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||jw}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||jw}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new I;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Mt();constructor(){let e=this._defaults,i=u(Nn);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Hn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Yt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(yt([void 0,void 0]),ne(()=>[i.errorState,i.userAriaDescribedBy]),Tl(),Ie(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(ke(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Xt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){eC({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Yt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,g=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${l1} translateX(${g}))`,S=s+a+c+l;return[b,S]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Wd(o,r._labelChild,dt,5),xn(o,kc,5)(o,Bw,5)(o,Uw,5)(o,o1,5)(o,Nc,5)),i&2){Yd();let s;K(s=Q())&&(r._formFieldControl=s.first),K(s=Q())&&(r._prefixChildren=s),K(s=Q())&&(r._suffixChildren=s),K(s=Q())&&(r._errorChildren=s),K(s=Q())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(qd(r._iconPrefixContainerSignal,Aw,5)(r._textPrefixContainerSignal,Rw,5)(r._iconSuffixContainerSignal,Nw,5)(r._textSuffixContainerSignal,kw,5),kt(jO,5)(Aw,5)(Rw,5)(Nw,5)(kw,5)(Ow,5)(Lw,5)(Pw,5)),i&2){Yd(4);let o;K(o=Q())&&(r._textField=o.first),K(o=Q())&&(r._iconPrefixContainer=o.first),K(o=Q())&&(r._textPrefixContainer=o.first),K(o=Q())&&(r._iconSuffixContainer=o.first),K(o=Q())&&(r._textSuffixContainer=o.first),K(o=Q())&&(r._floatingLabel=o.first),K(o=Q())&&(r._notchedOutline=o.first),K(o=Q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ce("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ze([{provide:Oc,useExisting:t},{provide:Hw,useExisting:t}])],ngContentSelectors:UO,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(He(BO),H(0,$O,1,1,"ng-template",null,0,ja),p(2,"div",6,1),F("click",function(s){return r._control.onContainerClick(s)}),_e(4,GO,1,0,"div",7),p(5,"div",8),_e(6,YO,2,2,"div",9),_e(7,ZO,3,0,"div",10),_e(8,KO,3,0,"div",11),p(9,"div",12),_e(10,XO,1,1,null,13),ue(11),h(),_e(12,JO,3,0,"div",14),_e(13,e1,3,0,"div",15),h(),_e(14,t1,1,0,"div",16),h(),p(15,"div",17),_e(16,n1,2,0,"div",18)(17,r1,5,1,"div",19),h()),i&2){let o;_(2),ce("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),_(2),ye(!r._hasOutline()&&!r._control.disabled?4:-1),_(2),ye(r._hasOutline()?6:-1),_(),ye(r._hasIconPrefix?7:-1),_(),ye(r._hasTextPrefix?8:-1),_(2),ye(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),_(2),ye(r._hasTextSuffix?12:-1),_(),ye(r._hasIconSuffix?13:-1),_(),ye(r._hasOutline()?-1:14),_(),ce("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();_(),ye((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[Ow,Lw,za,Pw,Nc],styles:[`.mdc-text-field {
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
`],encapsulation:2})}return t})();var Os=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Fs=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var et=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[dw,_t,Se]})}return t})();var d1=["button","checkbox","file","hidden","image","radio","range","reset","submit"],u1=new y("MAT_INPUT_CONFIG"),Bt=(()=>{class t{_elementRef=u(j);_platform=u($e);ngControl=u(li,{optional:!0,self:!0});_autofillMonitor=u(Sw);_ngZone=u(O);_formField=u(Oc,{optional:!0});_renderer=u(je);_uid=u(gt).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(u1,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new I;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=ks(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(go.required)??!1}set required(e){this._required=ks(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Xg().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=ks(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Xg().has(e));constructor(){let e=u(Kt,{optional:!0}),i=u(bc,{optional:!0}),r=u(Os),o=u(Mw,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?bn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Fs(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Hn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){d1.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&F("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Dn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),he("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ce("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},exportAs:["matInput"],features:[ze([{provide:kc,useExisting:t}]),nt]})}return t})(),Ut=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[et,et,Iw,Se]})}return t})();function zw(t){return Error(`Unable to find icon with the name "${t}"`)}function f1(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function $w(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function Gw(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Vi=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},qw=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Vi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(ct.HTML,r);if(!s)throw Gw(r);let a=yo(s);return this._addSvgIconConfig(e,i,new Vi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Vi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(ct.HTML,i);if(!o)throw Gw(i);let s=yo(o);return this._addSvgIconSetConfig(e,new Vi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(ct.RESOURCE_URL,e);if(!i)throw $w(e);let r=this._cachedIconsByUrl.get(i);return r?V(lf(r)):this._loadSvgIconFromConfig(new Vi(e,null)).pipe(Ze(o=>this._cachedIconsByUrl.set(i,o)),ne(o=>lf(o)))}getNamedSvgIcon(e,i=""){let r=Ww(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):ta(zw(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?V(lf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ne(i=>lf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return V(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Pr(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(ct.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),V(null)})));return ia(o).pipe(ne(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw zw(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(Ze(i=>e.svgText=i),ne(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?V(null):this._fetchIcon(e).pipe(Ze(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(yo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(yo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw f1();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(ct.RESOURCE_URL,i);if(!s)throw $w(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ne(l=>yo(l)),Lr(()=>this._inProgressUrlFetches.delete(s)),ra());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(Ww(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return h1(o)?new Vi(o.url,null,o.options):new Vi(o,null)}}static \u0275fac=function(i){return new(i||t)(M(Tn,8),M(Qa),M(U,8),M(qt))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function lf(t){return t.cloneNode(!0)}function Ww(t,n){return t+":"+n}function h1(t){return!!(t.url&&t.options)}var p1=["*"],m1=new y("MAT_ICON_DEFAULT_OPTIONS"),g1=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=u(U),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),Yw=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],v1=Yw.map(t=>`[${t}]`).join(", "),_1=/^url\(['"]?#(.*?)['"]?\)$/,Ht=(()=>{class t{_elementRef=u(j);_iconRegistry=u(qw);_location=u(g1);_errorHandler=u(qt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=fe.EMPTY;constructor(){let e=u(new ir("aria-hidden"),{optional:!0}),i=u(m1,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(v1),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)Yw.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(_1):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Wt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(he("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),cn(r.color?"mat-"+r.color:""),ce("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",B],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:p1,decls:1,vars:0,template:function(i,r){i&1&&(He(),ue(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2})}return t})(),zt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se]})}return t})();var Ge={production:!1,apiUrl:"http://localhost:8080/tela-login-angular"};var ot=class t{set(n,e){localStorage.setItem(n,JSON.stringify(e))}get(n){let e=localStorage.getItem(n);return e?JSON.parse(e):null}remove(n){localStorage.removeItem(n)}clear(){localStorage.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})};var Fn=class t{constructor(n,e){this.http=n;this.localStorageService=e}http;localStorageService;login(n,e){return this.http.post(`${Ge.apiUrl}/auth/login`,{email:n,senha:e}).pipe(Ze(i=>{this.localStorageService.set("token",i.token),this.localStorageService.set("id",i.id),this.localStorageService.set("nome",i.nome),this.localStorageService.set("email",i.email),this.localStorageService.set("role",i.role)}))}logout(){let n=localStorage.getItem("token");localStorage.clear(),n&&this.http.post(`${Ge.apiUrl}/auth/logout`,{},{headers:{Authorization:`Bearer ${n}`}}).subscribe()}getToken(){return this.localStorageService.get("token")}isLoggedIn(){return!!this.getToken()}novoUsuario(n,e,i){return this.http.post(`${Ge.apiUrl}/auth/usuario`,{nome:n,email:e,senha:i})}static \u0275fac=function(e){return new(e||t)(M(Tn),M(ot))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})};function y1(t,n){if(t&1&&(p(0,"div",14),v(1),h()),t&2){let e=D();_(),be(" ",e.mensagens," ")}}function b1(t,n){t&1&&(p(0,"mat-icon"),v(1," login "),h())}var df=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;email="";senha="";mensagens="";carregando=!1;onSubmit(){this.carregando=!0,this.mensagens="",this.authService.login(this.email,this.senha).subscribe({next:n=>{console.log("Login realizado com sucesso",n),this.carregando=!1,this.cdr.detectChanges(),this.router.navigate(["/pets"])},error:n=>{console.error("Falha no login",n),this.carregando=!1,n.status===401?this.mensagens=n.error?.erro??"Email ou senha invalidos.":n.status===0?this.mensagens=n.error?.erro??"Nao foi possivel conectar ao servidor.":this.mensagens=n.error?.erro??"Erro inesperado. Tente novamente.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Fn),w(Ve),w(me))};static \u0275cmp=R({type:t,selectors:[["app-login"]],decls:30,vars:6,consts:[[1,"login-container"],[1,"login-card"],[1,"login-header"],[1,"login-icon"],[3,"ngSubmit"],["appearance","outline"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["class","error",4,"ngIf"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[4,"ngIf"],[1,"switch-link"],["routerLink","/criar-usuario"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-icon",3),v(4," pets "),h(),p(5,"h2"),v(6,"Entrar"),h(),p(7,"p"),v(8,"Bem-vindo ao PetManager"),h()(),p(9,"form",4),F("ngSubmit",function(){return i.onSubmit()}),p(10,"mat-form-field",5)(11,"mat-label"),v(12,"Email"),h(),p(13,"input",6),Ne("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),p(14,"mat-icon",7),v(15," email "),h()(),p(16,"mat-form-field",5)(17,"mat-label"),v(18,"Senha"),h(),p(19,"input",8),Ne("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),p(20,"mat-icon",7),v(21," lock "),h()(),H(22,y1,2,1,"div",9),p(23,"button",10),H(24,b1,2,0,"mat-icon",11),v(25),h()(),p(26,"div",12),v(27," N\xE3o possui uma conta? "),p(28,"a",13),v(29," Criar conta "),h()()()()),e&2&&(_(13),Re("ngModel",i.email),Fe(),_(6),Re("ngModel",i.senha),Fe(),_(3),E("ngIf",i.mensagens),_(),E("disabled",i.carregando),_(),E("ngIf",!i.carregando),_(),be(" ",i.carregando?"Entrando...":"Entrar"," "))},dependencies:[Pt,ui,mt,Ft,di,Rn,xt,Kt,it,lt,rt,Vt,Lt,vt,jt,et,_t,dt,hn,Ut,Bt,zt,Ht],encapsulation:2})};function C1(t,n){if(t&1&&(p(0,"p"),v(1),h()),t&2){let e=D();_(),be(" ",e.mensagens," ")}}var uf=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;nome="";email="";senha="";mensagens="";onSubmit(){if(this.senha.length<4){this.mensagens="A senha deve ter no minimo 4 caracteres",this.cdr.detectChanges();return}this.authService.novoUsuario(this.nome,this.email,this.senha).subscribe({next:n=>{console.log("Usuario criado com sucesso",n),this.mensagens="Usuario criado com sucesso.",this.router.navigate(["/login"]),this.cdr.detectChanges()},error:n=>{console.error("Nao foi possivel criar este usuario",n),this.mensagens=n.error?.erro??"Nao foi possivel criar este usuario. Verifique se as informa\xE7\xF5es s\xE3o v\xE1lidas",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Fn),w(Ve),w(me))};static \u0275cmp=R({type:t,selectors:[["app-criar-usuario"]],decls:24,vars:4,consts:[[1,"login-container"],[1,"login-card"],[3,"ngSubmit"],["appearance","outline"],["matInput","","name","nome","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["mat-raised-button","","color","primary","type","submit",1,"full-width"],[4,"ngIf"],[1,"switch-link"],["routerLink","/login"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"h2"),v(3,"Criar conta"),h(),p(4,"form",2),F("ngSubmit",function(){return i.onSubmit()}),p(5,"mat-form-field",3)(6,"mat-label"),v(7,"Nome"),h(),p(8,"input",4),Ne("ngModelChange",function(o){return Pe(i.nome,o)||(i.nome=o),o}),h(),Oe(),h(),p(9,"mat-form-field",3)(10,"mat-label"),v(11,"Email"),h(),p(12,"input",5),Ne("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),h(),p(13,"mat-form-field",3)(14,"mat-label"),v(15,"Senha (m\xEDnimo de 4 caracteres)"),h(),p(16,"input",6),Ne("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),h(),p(17,"button",7),v(18," Criar conta "),h()(),H(19,C1,2,1,"p",8),p(20,"div",9),v(21," J\xE1 possui uma conta? "),p(22,"a",10),v(23," Entrar "),h()()()()),e&2&&(_(8),Re("ngModel",i.nome),Fe(),_(4),Re("ngModel",i.email),Fe(),_(4),Re("ngModel",i.senha),Fe(),_(3),E("ngIf",i.mensagens))},dependencies:[Pt,ui,mt,Ft,di,Rn,xt,Kt,it,lt,rt,Vt,Lt,et,_t,dt,Ut,Bt,vt,jt],styles:[".full-width[_ngcontent-%COMP%]{width:100%}.switch-link[_ngcontent-%COMP%]{margin-top:20px;text-align:center}.switch-link[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{font-weight:600}"]})};var E1=["determinateSpinner"];function w1(t,n){if(t&1&&(ni(),p(0,"svg",11),pe(1,"circle",12),h()),t&2){let e=D();he("viewBox",e._viewBox()),_(),so("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),he("r",e._circleRadius())}}var D1=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Zw})}),Zw=100,x1=10,Pn=(()=>{class t{_elementRef=u(j);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=u(D1),i=Jg(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Zw;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-x1)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&kt(E1,5),i&2){let o;K(o=Q())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(he("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),cn("mat-"+r.color),so("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),ce("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",In],diameter:[2,"diameter","diameter",In],strokeWidth:[2,"strokeWidth","strokeWidth",In]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(H(0,w1,2,8,"ng-template",null,0,ja),p(2,"div",2,1),ni(),p(4,"svg",3),pe(5,"circle",4),h()(),nd(),p(6,"div",5)(7,"div",6)(8,"div",7),cs(9,8),h(),p(10,"div",9),cs(11,8),h(),p(12,"div",10),cs(13,8),h()()()),i&2){let o=Sn(1);_(4),he("viewBox",r._viewBox()),_(),so("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),he("r",r._circleRadius()),_(4),E("ngTemplateOutlet",o),_(2),E("ngTemplateOutlet",o),_(2),E("ngTemplateOutlet",o)}},dependencies:[za],styles:[`.mat-mdc-progress-spinner {
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
`],encapsulation:2})}return t})();var Ln=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se]})}return t})();var S1=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],I1=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function M1(t,n){t&1&&(p(0,"span",3),ue(1,1),h())}function T1(t,n){t&1&&(p(0,"span",6),ue(1,2),h())}var A1=["*"];var R1=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),Kw=new y("MatChipAvatar"),Qw=new y("MatChipTrailingIcon"),Xw=new y("MatChipEdit"),sv=new y("MatChipRemove"),Jw=new y("MatChip"),eD=(()=>{class t{_elementRef=u(j);_parentChip=u(Jw);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u(It).load(Fi),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(he("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),ce("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:In(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),tD=(()=>{class t extends eD{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(he("tabindex",r._getTabindex()),ce("mdc-evolution-chip__action--presentational",!1))},features:[Be]})}return t})();var nD=(()=>{class t extends tD{_isPrimary=!1;_handleClick(e){this.disabled||(e.stopPropagation(),e.preventDefault(),this._parentChip.remove())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&(e.stopPropagation(),e.preventDefault(),this._parentChip.remove())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=qn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","matChipRemove",""]],hostAttrs:["role","button",1,"mat-mdc-chip-remove","mat-mdc-chip-trailing-icon","mat-focus-indicator","mdc-evolution-chip__icon","mdc-evolution-chip__icon--trailing"],hostVars:1,hostBindings:function(i,r){i&2&&he("aria-hidden",null)},features:[ze([{provide:sv,useExisting:t}]),Be]})}return t})(),Lc=(()=>{class t{_changeDetectorRef=u(me);_elementRef=u(j);_tagName=u(K0);_ngZone=u(O);_focusMonitor=u(hr);_globalRippleOptions=u(Rc,{optional:!0});_document=u(U);_onFocus=new I;_onBlur=new I;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Mt();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(gt).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new Y;destroyed=new Y;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(af);_injector=u(ae);constructor(){let e=u(It);e.load(Fi),e.load(wc),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Xt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&xn(o,Kw,5)(o,Xw,5)(o,Qw,5)(o,sv,5)(o,Kw,5)(o,Qw,5)(o,Xw,5)(o,sv,5),i&2){let s;K(s=Q())&&(r.leadingIcon=s.first),K(s=Q())&&(r.editIcon=s.first),K(s=Q())&&(r.trailingIcon=s.first),K(s=Q())&&(r.removeIcon=s.first),K(s=Q())&&(r._allLeadingIcons=s),K(s=Q())&&(r._allTrailingIcons=s),K(s=Q())&&(r._allEditIcons=s),K(s=Q())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&kt(tD,5),i&2){let o;K(o=Q())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Dn("id",r.id),he("role",r.role)("aria-label",r.ariaLabel),cn("mat-"+(r.color||"primary")),ce("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",B],highlighted:[2,"highlighted","highlighted",B],disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[ze([{provide:Jw,useExisting:t}])],ngContentSelectors:I1,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(He(S1),pe(0,"span",0),p(1,"span",1)(2,"span",2),_e(3,M1,2,0,"span",3),p(4,"span",4),ue(5),pe(6,"span",5),h()()(),_e(7,T1,2,0,"span",6)),i&2&&(_(3),ye(r.leadingIcon?3:-1),_(4),ye(r._hasTrailingIcon()?7:-1))},dependencies:[eD],styles:[`.mdc-evolution-chip,
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
`],encapsulation:2})}return t})();var ff=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_dir=u(Nn,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new I;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new Gn;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(yt(null),We(()=>Xt(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(yt(this._chips)).subscribe(e=>{let i=[];e.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new Co(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(ke(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(ke(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(yt(null),ke(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(ke(this._destroyed)).subscribe(e=>{let r=this._chips.toArray().indexOf(e.chip),o=e.chip._hasFocus(),s=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&xn(o,Lc,5),i&2){let s;K(s=Q())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)}),i&2&&he("role",r.role)},inputs:{disabled:[2,"disabled","disabled",B],role:"role",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:In(e)]},ngContentSelectors:A1,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(He(),ht(0,"div",0),ue(1),Dt())},styles:[`.mat-mdc-chip-set {
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
`],encapsulation:2})}return t})();var Ls=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({providers:[Os,{provide:R1,useValue:{separatorKeyCodes:[13]}}],imports:[pr,Se]})}return t})();var Vc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},av=class extends Vc{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Do=class extends Vc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},cv=class extends Vc{element;constructor(n){super(),this.element=n instanceof j?n.nativeElement:n}},lv=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof av)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Do)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof cv)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},jc=class extends lv{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Si,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ae.NULL,o=r.get(xe,i.injector);e=Jd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var rD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})();var N1=20,hf=(()=>{class t{_ngZone=u(O);_platform=u($e);_renderer=u(tt).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new I;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=N1){return this._platform.isBrowser?new re(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Sl(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):V()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Ie(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=fn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var k1=20,yr=(()=>{class t{_platform=u($e);_listeners;_viewportSize=null;_change=new I;_document=u(U);constructor(){let e=u(O),i=u(tt).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=k1){return e>0?this._change.pipe(Sl(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Vs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({})}return t})(),dv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se,Vs,Se,Vs]})}return t})();var oD=mw();function fD(t){return new pf(t.get(yr),t.get(U))}var pf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Je(-this._previousScrollPosition.left),n.style.top=Je(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),oD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),oD&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function hD(t,n){return new mf(t.get(hf),t.get(O),t.get(yr),n)}var mf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ie(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Bc=class{enable(){}disable(){}attach(){}};function uv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function sD(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Io(t,n){return new gf(t.get(hf),t.get(yr),t.get(O),n)}var gf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();uv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},pD=(()=>{class t{_injector=u(ae);noop=()=>new Bc;close=e=>hD(this._injector,e);block=()=>fD(this._injector);reposition=e=>Io(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),So=class{positionStrategy;scrollStrategy=new Bc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var vf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var mD=(()=>{class t{_attachedOverlays=[];_document=u(U);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),gD=(()=>{class t extends mD{_ngZone=u(O);_renderer=u(tt).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),vD=(()=>{class t extends mD{_platform=u($e);_ngZone=u(O);_renderer=u(tt).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=St(e)};_clickListener=e=>{let i=St(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(aD(a.overlayElement,i)||aD(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function aD(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var _D=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2})}return t})(),yD=(()=>{class t{_platform=u($e);_containerElement;_document=u(U);_styleLoader=u(It);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Qg()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Qg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(_D)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),fv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function hv(t){return t&&t.nodeType===1}var _f=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new I;_attachments=new I;_detachments=new I;_positionStrategy;_scrollStrategy;_locationChanges=fe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new I;_outsidePointerEvents=new I;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,d=!1,f,m){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=m,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=En(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=z(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Je(this._config.width),n.height=Je(this._config.height),n.minWidth=Je(this._config.minWidth),n.minHeight=Je(this._config.minHeight),n.maxWidth=Je(this._config.maxWidth),n.maxHeight=Je(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;hv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(n){}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new fv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Yg(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=En(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},cD="cdk-overlay-connected-position-bounding-box",O1=/([A-Za-z%]+)$/;function Uc(t,n){return new yf(n,t.get(yr),t.get(U),t.get($e),t.get(yD))}var yf=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new I;_resizeSubscription=fe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(cD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&xo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(cD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof j?this._origin.nativeElement:hv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=dD(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,m=0-a,g=a+o.height-i.height,b=this._subtractOverflows(o.width,d,f),S=this._subtractOverflows(o.height,m,g),N=b*S;return{visibleArea:N,isCompletelyWithinViewport:o.width*o.height===N,fitsInViewportVertically:S===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=lD(this._overlayRef.getConfig().minHeight),a=lD(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=dD(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!F1(this._lastScrollVisibility,i)){let r=new vf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let g=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=g*2,s=n.y-g,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-b/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,m;if(l)m=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let g=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;d=g*2,f=n.x-g,d>b&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-b/2)}return{top:s,left:f,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Je(i.width),r.height=Je(i.height),r.top=Je(i.top)||"auto",r.bottom=Je(i.bottom)||"auto",r.left=Je(i.left)||"auto",r.right=Je(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Je(o)),s&&(r.maxWidth=Je(s))}this._lastBoundingBoxSize=i,xo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){xo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){xo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();xo(i,this._getExactOverlayY(e,n,d)),xo(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Je(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Je(s.maxWidth):o&&(i.maxWidth="")),xo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Je(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Je(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:sD(n,i),isOriginOutsideView:uv(n,i),isOverlayClipped:sD(e,i),isOverlayOutsideView:uv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Yg(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof j)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function xo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function lD(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(O1);return!e||e==="px"?parseFloat(n):null}return t||null}function dD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function F1(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var uD="cdk-global-overlay-wrapper";function bD(t){return new bf}var bf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(uD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",g="",b="",S="";c?S="flex-start":d==="center"?(S="center",m?b=f:g=f):m?d==="left"||d==="end"?(S="flex-end",g=f):(d==="right"||d==="start")&&(S="flex-start",b=f):d==="left"||d==="start"?(S="flex-start",g=f):(d==="right"||d==="end")&&(S="flex-end",b=f),n.position=this._cssPosition,n.marginLeft=c?"0":g,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":b,e.justifyContent=S,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(uD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},CD=(()=>{class t{_injector=u(ae);global(){return bD()}flexibleConnectedTo(e){return Uc(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Hc=new y("OVERLAY_DEFAULT_CONFIG");function zc(t,n){t.get(It).load(_D);let e=t.get(yD),i=t.get(U),r=t.get(gt),o=t.get(tn),s=t.get(Nn),a=t.get(je,null,{optional:!0})||t.get(tt).createRenderer(null,null),c=new So(n),l=t.get(Hc,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return hv(m)?m.after(f):m?.type==="parent"?m.element.appendChild(f):e.getContainerElement().appendChild(f),new _f(new jc(d,o,t),f,d,c,t.get(O),t.get(gD),i,t.get(or),t.get(vD),n?.disableAnimations??t.get(ya,null,{optional:!0})==="NoopAnimations",t.get(xe),a)}var ED=(()=>{class t{scrollStrategies=u(pD);_positionBuilder=u(CD);_injector=u(ae);create(e){return zc(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),P1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],L1=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>Io(t)}}),js=(()=>{class t{elementRef=u(j);static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),wD=new y("cdk-connected-overlay-default-config"),Ef=(()=>{class t{_dir=u(Nn,{optional:!0});_injector=u(ae);_overlayRef;_templatePortal;_backdropSubscription=fe.EMPTY;_attachSubscription=fe.EMPTY;_detachSubscription=fe.EMPTY;_positionSubscription=fe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(L1);_ngZone=u(O);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new Y;positionChange=new Y;attach=new Y;detach=new Y;overlayKeydown=new Y;overlayOutsideClick=new Y;constructor(){let e=u(en),i=u(Nt),r=u(wD,{optional:!0}),o=u(Hc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Do(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=P1);let e=this._overlayRef=zc(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!kn(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=St(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new So({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Uc(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof js?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof js?this.origin.elementRef.nativeElement:this.origin instanceof j?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(nh(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",B],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",B],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",B],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",B],push:[2,"cdkConnectedOverlayPush","push",B],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",B],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",B],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[nt]})}return t})(),$c=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({providers:[ED],imports:[Se,rD,dv,dv]})}return t})();var V1=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],j1=["mat-icon, [matMenuItemIcon]","*"];function B1(t,n){t&1&&(ni(),p(0,"svg",2),pe(1,"polygon",3),h())}var U1=["*"];function H1(t,n){if(t&1){let e=Ue();ht(0,"div",0),Gd("click",function(){le(e);let r=D();return de(r.closed.emit("click"))})("animationstart",function(r){le(e);let o=D();return de(o._onAnimationStart(r.animationName))})("animationend",function(r){le(e);let o=D();return de(o._onAnimationDone(r.animationName))})("animationcancel",function(r){le(e);let o=D();return de(o._onAnimationDone(r.animationName))}),ht(1,"div",1),ue(2),Dt()()}if(t&2){let e=D();cn(e._classList),ce("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Dn("id",e.panelId),he("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var mv=new y("MAT_MENU_PANEL"),Mo=(()=>{class t{_elementRef=u(j);_document=u(U);_focusMonitor=u(hr);_parentMenu=u(mv,{optional:!0});_changeDetectorRef=u(me);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new I;_focused=new I;_highlighted=!1;_triggersSubmenu=!1;constructor(){u(It).load(Fi),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&F("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(he("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),ce("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B]},exportAs:["matMenuItem"],ngContentSelectors:j1,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(He(V1),ue(0),p(1,"span",0),ue(2,1),h(),pe(3,"div",1),_e(4,B1,2,0,":svg:svg",2)),i&2&&(_(3),E("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),_(),ye(r._triggersSubmenu?4:-1))},dependencies:[of],encapsulation:2})}return t})();var z1=new y("MatMenuContent");var $1=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),pv="_mat-menu-enter",wf="_mat-menu-exit",br=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_injector=u(ae);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Mt();_allItems;_directDescendantItems=new Gn;_classList={};_panelAnimationState="void";_animationDone=new I;_isAnimating=ee(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=C({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new Y;close=this.closed;panelId=u(gt).getId("mat-menu-panel-");constructor(){let e=u($1);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Co(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(yt(this._directDescendantItems),We(e=>Xt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(yt(this._directDescendantItems),We(i=>Xt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:kn(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=En(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=z(C({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===wf;(i||e===pv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===pv||e===wf)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(wf),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?pv:wf)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(yt(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&xn(o,z1,5)(o,Mo,5)(o,Mo,4),i&2){let s;K(s=Q())&&(r.lazyContent=s.first),K(s=Q())&&(r._allItems=s),K(s=Q())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&kt(en,5),i&2){let o;K(o=Q())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&he("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",B],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:B(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[ze([{provide:mv,useExisting:t}])],ngContentSelectors:U1,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(He(),zd(0,H1,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2})}return t})(),G1=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>Io(t)}});var Bs=new WeakMap,W1=(()=>{class t{_canHaveBackdrop;_element=u(j);_viewContainerRef=u(Nt);_menuItemInstance=u(Mo,{optional:!0,self:!0});_dir=u(Nn,{optional:!0});_focusMonitor=u(hr);_ngZone=u(O);_injector=u(ae);_scrollStrategy=u(G1);_changeDetectorRef=u(me);_animationsDisabled=Mt();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=fe.EMPTY;_menuCloseSubscription=fe.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=u(mv,{optional:!0});this._parentMaterialMenu=i instanceof br?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Bs.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Bs.get(i);Bs.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof br&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(ke(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof br&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Wt(1)).subscribe(()=>{i.detach(),Bs.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Bs.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=zc(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof br&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new So({positionStrategy:Uc(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[s,a],[d,f]=[r,o],m=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=d=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let g=this._parentMaterialMenu.items.first;this._parentInnerPadding=g?g._getHostElement().offsetTop:0}m=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=s==="top"?"bottom":"top",l=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:d,overlayY:s,offsetY:m},{originX:o,originY:c,overlayX:f,overlayY:s,offsetY:m},{originX:r,originY:l,overlayX:d,overlayY:a,offsetY:-m},{originX:o,originY:l,overlayX:f,overlayY:a,offsetY:-m}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:V(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Ie(s=>this._menuOpen&&s!==this._menuItemInstance)):V();return Xt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Do(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Bs.get(e)===this}_triggerIsAriaDisabled(){return B(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){Fa()};static \u0275dir=k({type:t})}return t})(),Df=(()=>{class t extends W1{_cleanupTouchstart;_hoverSubscription=fe.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new Y;onMenuOpen=this.menuOpened;menuClosed=new Y;onMenuClose=this.menuClosed;constructor(){super(!0);let e=u(je);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{_o(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){vo(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&he("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[Be]})}return t})();var xf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[pr,$c,Se,Vs]})}return t})();var Cr=class t{constructor(n){this.http=n}http;todos(){return this.http.get(`${Ge.apiUrl}/usuario/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/usuario/${n}`)}porEmail(n){return this.http.get(`${Ge.apiUrl}/usuario`,{params:{email:n}})}atualizar(n){return this.http.put(`${Ge.apiUrl}/usuario`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/usuario/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Tn))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})};var q1=t=>["/usuarios",t];function Y1(t,n){if(t&1){let e=Ue();p(0,"button",11),F("click",function(){le(e);let r=D();return r.termoBusca="",de(r.filtrarUsuarios())}),p(1,"mat-icon"),v(2,"close"),h()()}}function Z1(t,n){if(t&1&&(p(0,"div",12),v(1),h()),t&2){let e=D();_(),be(" ",e.mensagens," ")}}function K1(t,n){t&1&&(p(0,"div",13),pe(1,"mat-spinner",14),h())}function Q1(t,n){t&1&&(p(0,"div",26),v(1," Empresas relacionadas "),h())}function X1(t,n){if(t&1&&(p(0,"button",27)(1,"mat-icon"),v(2,"work_outline"),h(),p(3,"span"),v(4),h()()),t&2){let e=n.$implicit;_(4),Xe(e.nome)}}function J1(t,n){t&1&&(p(0,"div",28),v(1," Nenhuma empresa vinculada "),h())}function eF(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),v(2),h(),p(3,"td"),v(4),h(),p(5,"td")(6,"mat-chip-set")(7,"mat-chip",18),v(8),h()()(),p(9,"td",19)(10,"button",20)(11,"mat-icon"),v(12,"work"),h()(),p(13,"mat-menu",null,0),H(15,Q1,2,0,"div",21)(16,X1,5,1,"button",22)(17,J1,2,0,"div",23),h(),p(18,"button",24)(19,"mat-icon"),v(20,"edit"),h()(),p(21,"button",25),F("click",function(){let r=le(e).$implicit,o=D(2);return de(o.remover(r.id))}),p(22,"mat-icon"),v(23,"delete"),h()()()()}if(t&2){let e=n.$implicit,i=Sn(14);_(2),Xe(e.nome),_(2),Xe(e.email),_(3),E("color",e.role==="ADMIN"?"primary":"accent"),_(),be(" ",e.role," "),_(2),E("matMenuTriggerFor",i),_(5),E("ngIf",e.empresas?.length),_(),E("ngForOf",e.empresas),_(),E("ngIf",!e.empresas?.length),_(),E("routerLink",ao(9,q1,e.id))}}function tF(t,n){if(t&1&&(p(0,"table",15)(1,"thead")(2,"tr")(3,"th"),v(4,"Nome"),h(),p(5,"th"),v(6,"Email"),h(),p(7,"th"),v(8,"Perfil"),h(),p(9,"th",16),v(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),H(12,eF,24,11,"tr",17),h()()),t&2){let e=D();_(12),E("ngForOf",e.usuariosFiltrados)}}function nF(t,n){t&1&&(p(0,"p"),v(1,"N\xE3o h\xE1 usu\xE1rios cadastrados."),h())}function iF(t,n){t&1&&(p(0,"p"),v(1,"Tente outro termo de busca."),h())}function rF(t,n){if(t&1&&(p(0,"div",29)(1,"mat-icon",30),v(2," group "),h(),p(3,"h3"),v(4),h(),H(5,nF,2,0,"p",31)(6,iF,2,0,"p",31),h()),t&2){let e=D();_(4),Xe((e.termoBusca,"Nenhum usu\xE1rio encontrado")),_(),E("ngIf",!e.termoBusca),_(),E("ngIf",e.termoBusca)}}var Sf=class t{constructor(n,e,i,r,o){this.usuarioService=n;this.authService=e;this.localStorageService=i;this.cdr=r;this.router=o}usuarioService;authService;localStorageService;cdr;router;usuarios=[];carregando=!0;mensagens="";usuariosFiltrados=[];termoBusca="";ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.usuarioService.todos().subscribe({next:n=>{this.usuarios=n,this.filtrarUsuarios(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar usu\xE1rios.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarUsuarios(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.usuariosFiltrados=this.usuarios;return}this.usuariosFiltrados=this.usuarios.filter(e=>e.nome?.toLowerCase().includes(n)||e.email?.toLowerCase().includes(n)||e.role?.toLowerCase().includes(n))}remover(n){if(n==null||!confirm("Confirma\xE7ao para remover este usuario"))return;let e=Number(this.localStorageService.get("id"))===n;this.usuarioService.remover(n).subscribe({next:()=>{if(e){this.authService.logout(),this.router.navigate(["/login"]),this.cdr.detectChanges();return}this.usuarios=this.usuarios.filter(i=>i.id!==n),this.filtrarUsuarios(),this.cdr.detectChanges()},error:i=>{console.error("Erro ao remover usuario",i),this.mensagens="Nao foi possivel remover este usuario.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Cr),w(Fn),w(ot),w(me),w(Ve))};static \u0275cmp=R({type:t,selectors:[["app-gestao-usuarios"]],decls:18,vars:6,consts:[["menuEmpresasRelacionadas","matMenu"],[1,"page"],[1,"header"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, email ou perfil...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],["selected","",3,"color"],[1,"actions"],["mat-icon-button","","color","accent","matTooltip","Empresas relacionadas",3,"matMenuTriggerFor"],["class","menu-header",4,"ngIf"],["mat-menu-item","",4,"ngFor","ngForOf"],["class","menu-empty",4,"ngIf"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"menu-header"],["mat-menu-item",""],[1,"menu-empty"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",1)(1,"mat-card")(2,"div",2)(3,"h2")(4,"mat-icon"),v(5,"group"),h(),v(6," Gest\xE3o de Usu\xE1rios "),h()(),p(7,"mat-form-field",3)(8,"mat-label"),v(9,"Pesquisar"),h(),p(10,"input",4),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.filtrarUsuarios()}),h(),Oe(),p(11,"mat-icon",5),v(12,"search"),h(),H(13,Y1,3,0,"button",6),h(),H(14,Z1,2,1,"div",7)(15,K1,2,0,"div",8)(16,tF,13,1,"table",9)(17,rF,7,3,"div",10),h()()),e&2&&(_(10),Re("ngModel",i.termoBusca),Fe(),_(3),E("ngIf",i.termoBusca),_(),E("ngIf",i.mensagens),_(),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando&&i.usuariosFiltrados.length>0),_(),E("ngIf",!i.carregando&&i.usuariosFiltrados.length===0))},dependencies:[it,nn,lt,xf,br,Mo,Df,Pt,mt,Ft,xt,rt,Vt,Lt,vt,Pi,zt,Ht,Ln,Pn,et,_t,dt,wo,hn,Ut,Bt,Ls,Lc,ff],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};var Gc=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new I;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var xD=(()=>{class t{_animationsDisabled=Mt();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ce("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2})}return t})();var oF=["text"],sF=[[["mat-icon"]],"*"],aF=["mat-icon","*"];function cF(t,n){if(t&1&&pe(0,"mat-pseudo-checkbox",1),t&2){let e=D();E("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function lF(t,n){if(t&1&&pe(0,"mat-pseudo-checkbox",3),t&2){let e=D();E("disabled",e.disabled)}}function dF(t,n){if(t&1&&(p(0,"span",4),v(1),h()),t&2){let e=D();_(),be("(",e.group.label,")")}}var _v=new y("MAT_OPTION_PARENT_COMPONENT"),yv=new y("MatOptgroup");var vv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},Er=(()=>{class t{_element=u(j);_changeDetectorRef=u(me);_parent=u(_v,{optional:!0});group=u(yv,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(gt).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ee(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new Y;_text;_stateChanges=new I;constructor(){let e=u(It);e.load(Fi),e.load(wc),this._signalDisableRipple=!!this._parent&&bn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!kn(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new vv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&kt(oF,7),i&2){let o;K(o=Q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&F("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Dn("id",r.id),he("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ce("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",B]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:aF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(He(sF),_e(0,cF,1,2,"mat-pseudo-checkbox",1),ue(1),p(2,"span",2,0),ue(4,1),h(),_e(5,lF,1,1,"mat-pseudo-checkbox",3),_e(6,dF,2,1,"span",4),pe(7,"div",5)),i&2&&(ye(r.multiple?0:-1),_(5),ye(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),_(),ye(r.group&&r.group._inert?6:-1),_(),E("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[xD,of],styles:[`.mat-mdc-option {
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
`],encapsulation:2})}return t})();function SD(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function ID(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var MD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[Se]})}return t})();var bv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[pr,MD,Er,Se]})}return t})();var uF=["trigger"],fF=["panel"],hF=[[["mat-select-trigger"]],"*"],pF=["mat-select-trigger","*"];function mF(t,n){if(t&1&&(p(0,"span",4),v(1),h()),t&2){let e=D();_(),Xe(e.placeholder)}}function gF(t,n){t&1&&ue(0)}function vF(t,n){if(t&1&&(p(0,"span",11),v(1),h()),t&2){let e=D(2);_(),Xe(e.triggerValue)}}function _F(t,n){if(t&1&&(p(0,"span",5),_e(1,gF,1,0)(2,vF,2,1,"span",11),h()),t&2){let e=D();_(),ye(e.customTrigger?1:2)}}function yF(t,n){if(t&1){let e=Ue();p(0,"div",12,1),F("keydown",function(r){le(e);let o=D();return de(o._handleKeydown(r))}),ue(2,1),h()}if(t&2){let e=D();cn(e.panelClass),ce("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),he("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var bF=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>Io(t)}}),CF=new y("MAT_SELECT_CONFIG"),EF=new y("MatSelectTrigger"),Cv=class{source;value;constructor(n,e){this.source=n,this.value=e}},If=(()=>{class t{_viewportRuler=u(yr);_changeDetectorRef=u(me);_elementRef=u(j);_dir=u(Nn,{optional:!0});_idGenerator=u(gt);_renderer=u(je);_parentFormField=u(Oc,{optional:!0});ngControl=u(li,{self:!0,optional:!0});_liveAnnouncer=u(Kg);_defaultOptions=u(CF,{optional:!0});_animationsDisabled=Mt();_popoverLocation;_initialized=new I;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=SD(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=ID(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Cv(this,e)}_scrollStrategyFactory=u(bF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new I;_errorStateTracker;stateChanges=new I;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=ee(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(go.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=Fr(()=>{let e=this.options;return e?e.changes.pipe(yt(e),We(()=>Xt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(We(()=>this.optionSelectionChanges))});openedChange=new Y;_openedStream=this.openedChange.pipe(Ie(e=>e),ne(()=>{}));_closedStream=this.openedChange.pipe(Ie(e=>!e),ne(()=>{}));selectionChange=new Y;valueChange=new Y;constructor(){let e=u(Os),i=u(Kt,{optional:!0}),r=u(bc,{optional:!0}),o=u(new ir("tabindex"),{optional:!0}),s=u(Hc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Fs(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Gc(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(ke(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(ke(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(yt(null),ke(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Wt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!kn(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!kn(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!kn(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch(o){return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof js?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Mc(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Xt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(ke(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Xt(...this.options.map(i=>i._stateChanges)).pipe(ke(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=St(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&xn(o,EF,5)(o,Er,5)(o,yv,5),i&2){let s;K(s=Q())&&(r.customTrigger=s.first),K(s=Q())&&(r.options=s),K(s=Q())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&kt(uF,5)(fF,5)(Ef,5),i&2){let o;K(o=Q())&&(r.trigger=o.first),K(o=Q())&&(r.panel=o.first),K(o=Q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(he("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),ce("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:In(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",B],placeholder:"placeholder",required:[2,"required","required",B],multiple:[2,"multiple","multiple",B],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",B],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",In],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",B]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[ze([{provide:kc,useExisting:t},{provide:_v,useExisting:t}]),nt],ngContentSelectors:pF,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(He(hF),p(0,"div",2,0),F("click",function(){return r.open()}),p(3,"div",3),_e(4,mF,2,1,"span",4)(5,_F,3,1,"span",5),h(),p(6,"div",6)(7,"div",7),ni(),p(8,"svg",8),pe(9,"path",9),h()()()(),H(10,yF,3,16,"ng-template",10),F("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=Sn(1);_(3),he("id",r._valueId),_(),ye(r.empty?4:5),_(6),E("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[js,Ef],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2})}return t})();var Us=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=$({imports:[$c,bv,Se,Vs,et,bv]})}return t})();function wF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function DF(t,n){if(t&1&&(p(0,"mat-option",17),v(1),h()),t&2){let e=n.$implicit;E("value",e),_(),be(" ",e," ")}}function xF(t,n){if(t&1&&(p(0,"div",18),v(1),h()),t&2){let e=D(2);_(),be(" ",e.mensagens," ")}}function SF(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=D();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=D();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),v(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.usuario.nome,r)||(o.usuario.nome=r),de(r)}),h(),Oe(),p(5,"mat-icon",8),v(6,"person"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),v(9,"Email"),h(),p(10,"input",9),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.usuario.email,r)||(o.usuario.email=r),de(r)}),h(),Oe(),p(11,"mat-icon",8),v(12,"email"),h()(),p(13,"mat-form-field",6)(14,"mat-label"),v(15,"Nova senha"),h(),p(16,"input",10),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.usuario.senha,r)||(o.usuario.senha=r),de(r)}),h(),Oe(),p(17,"mat-icon",8),v(18,"lock"),h(),p(19,"mat-hint"),v(20," Deixe em branco para manter a senha atual. "),h()(),p(21,"mat-form-field",6)(22,"mat-label"),v(23,"Perfil"),h(),p(24,"mat-select",11),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.usuario.role,r)||(o.usuario.role=r),de(r)}),H(25,DF,2,2,"mat-option",12),h(),Oe(),p(26,"mat-icon",8),v(27,"admin_panel_settings"),h()(),H(28,xF,2,1,"div",13),p(29,"div",14)(30,"button",15)(31,"mat-icon"),v(32,"save"),h(),v(33," Salvar "),h(),p(34,"button",16)(35,"mat-icon"),v(36,"arrow_back"),h(),v(37," Voltar "),h()()()}if(t&2){let e=D();_(4),Re("ngModel",e.usuario.nome),Fe(),_(6),Re("ngModel",e.usuario.email),Fe(),_(6),Re("ngModel",e.usuario.senha),Fe(),_(8),Re("ngModel",e.usuario.role),Fe(),_(),E("ngForOf",e.roles),_(3),E("ngIf",e.mensagens)}}var Mf=class t{constructor(n,e,i,r,o,s){this.usuarioService=n;this.authService=e;this.localStorageService=i;this.route=r;this.router=o;this.cdr=s}usuarioService;authService;localStorageService;route;router;cdr;roles=["USER","ADMIN"];usuario={nome:"",email:"",senha:"",role:"USER"};carregando=!0;mensagens="";autoEdicao=!1;ngOnInit(){let n=this.route.snapshot.paramMap.get("id"),e=this.localStorageService.get("id");console.log("idParam:",n),console.log("idLogado:",e);let i=localStorage.getItem("role")==="USER"||!n?e:n;console.log("id alvo:",i),i?this.usuarioService.porId(Number(i)).subscribe({next:r=>{this.usuario=z(C({},r),{senha:""}),this.autoEdicao=e!=null&&String(r.id)===e,this.carregando=!1,this.cdr.detectChanges()},error:r=>{console.error("Erro ao carregar usuario",r),this.mensagens="Usuario nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}):this.carregando=!1}onSubmit(){let n=C({},this.usuario);if(!n.senha)delete n.senha;else if(n.senha.length<4){this.mensagens="A senha deve ter no minimo 4 caracteres",this.cdr.detectChanges();return}this.usuarioService.atualizar(n).subscribe({next:()=>{if(this.autoEdicao){this.authService.logout(),this.router.navigate(["/login"]);return}this.router.navigate(["/usuarios"])},error:e=>{console.error("Erro ao salvar usuario",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o usuario.",this.cdr.detectChanges()}}),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(w(Cr),w(Fn),w(ot),w(Zt),w(Ve),w(me))};static \u0275cmp=R({type:t,selectors:[["app-usuario-form"]],decls:8,vars:2,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","email","id","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","id","senha","name","senha",3,"ngModelChange","ngModel"],["id","role","name","role",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","success",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/usuarios","type","button"],[3,"value"],[1,"success"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2")(3,"mat-icon"),v(4,"person"),h(),v(5," Editar usu\xE1rio "),h(),H(6,wF,2,0,"div",1)(7,SF,38,6,"form",2),h()()),e&2&&(_(6),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando))},dependencies:[it,nn,lt,Pt,ui,mt,Ft,di,Rn,xt,Kt,rt,Vt,Lt,et,_t,dt,Nc,hn,Ut,Bt,Us,If,Er,vt,jt,zt,Ht,Ln,Pn],encapsulation:2})};var Hs=class t{constructor(n){this.http=n}http;listar(){return this.http.get(`${Ge.apiUrl}/pet/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/pet/${n}`)}criar(n){return this.http.post(`${Ge.apiUrl}/pet`,n)}atualizar(n){return this.http.put(`${Ge.apiUrl}/pet`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/pet/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Tn))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})};var IF=t=>["/pets",t];function MF(t,n){if(t&1){let e=Ue();p(0,"button",11),F("click",function(){le(e);let r=D();return r.termoBusca="",de(r.buscarPets())}),p(1,"mat-icon"),v(2,"close"),h()()}}function TF(t,n){if(t&1&&(p(0,"div",12),v(1),h()),t&2){let e=D();_(),be(" ",e.mensagens," ")}}function AF(t,n){t&1&&(p(0,"div",13),pe(1,"mat-spinner",14),h())}function RF(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),v(2),h(),p(3,"td"),v(4),h(),p(5,"td"),v(6),h(),p(7,"td",18)(8,"button",19)(9,"mat-icon"),v(10,"edit"),h()(),p(11,"button",20),F("click",function(){let r=le(e).$implicit,o=D(2);return de(o.remover(r.id))}),p(12,"mat-icon"),v(13,"delete"),h()()()()}if(t&2){let e=n.$implicit;_(2),Xe(e.nome),_(2),Xe(e.especie),_(2),Xe(e.dono?.email??"-"),_(2),E("routerLink",ao(4,IF,e.id))}}function NF(t,n){if(t&1&&(p(0,"table",15)(1,"thead")(2,"tr")(3,"th"),v(4,"Nome"),h(),p(5,"th"),v(6,"Esp\xE9cie"),h(),p(7,"th"),v(8,"Dono"),h(),p(9,"th",16),v(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),H(12,RF,14,6,"tr",17),h()()),t&2){let e=D();_(12),E("ngForOf",e.petsBuscados)}}function kF(t,n){t&1&&(p(0,"p"),v(1,"Clique em "),p(2,"strong"),v(3,"Novo Pet"),h(),v(4," para come\xE7ar."),h())}function OF(t,n){t&1&&(p(0,"p"),v(1,"Tente outro termo de busca."),h())}function FF(t,n){if(t&1&&(p(0,"div",21)(1,"mat-icon",22),v(2,"pets"),h(),p(3,"h3"),v(4),h(),H(5,kF,5,0,"p",23)(6,OF,2,0,"p",23),h()),t&2){let e=D();_(4),Xe(e.termoBusca?"Nenhum pet encontrado":"Nenhum pet cadastrado"),_(),E("ngIf",!e.termoBusca),_(),E("ngIf",e.termoBusca)}}var Tf=class t{constructor(n,e){this.petService=n;this.cdr=e}petService;cdr;pets=[];carregando=!0;mensagens="";termoBusca="";petsBuscados=[];ngOnInit(){this.carregar()}buscarPets(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.petsBuscados=this.pets;return}this.petsBuscados=this.pets.filter(e=>e.nome?.toLowerCase().includes(n)||e.especie?.toLowerCase().includes(n)||e.dono?.email?.toLowerCase().includes(n))}carregar(){this.carregando=!0,this.petService.listar().subscribe({next:n=>{this.pets=n,this.buscarPets(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Nao foi possivel carregar os pets.",this.carregando=!1,this.cdr.detectChanges()}})}remover(n){n!=null&&confirm("Confirma\xE7ao para remover")&&this.petService.remover(n).subscribe({next:()=>{this.pets=this.pets.filter(e=>e.id!==n),this.buscarPets(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover pet",e),this.mensagens="Nao foi possivel remover este pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Hs),w(me))};static \u0275cmp=R({type:t,selectors:[["app-gestao-pets"]],decls:22,vars:6,consts:[[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/pets/novo"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, esp\xE9cie ou dono...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2")(4,"mat-icon"),v(5,"pets"),h(),v(6," Gest\xE3o de Pets "),h(),p(7,"button",2)(8,"mat-icon"),v(9,"add"),h(),v(10," Novo Pet "),h()(),p(11,"mat-form-field",3)(12,"mat-label"),v(13,"Pesquisar"),h(),p(14,"input",4),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.buscarPets()}),h(),Oe(),p(15,"mat-icon",5),v(16,"search"),h(),H(17,MF,3,0,"button",6),h(),H(18,TF,2,1,"div",7)(19,AF,2,0,"div",8)(20,NF,13,1,"table",9)(21,FF,7,3,"div",10),h()()),e&2&&(_(14),Re("ngModel",i.termoBusca),Fe(),_(3),E("ngIf",i.termoBusca),_(),E("ngIf",i.mensagens),_(),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando&&i.petsBuscados.length>0),_(),E("ngIf",!i.carregando&&i.petsBuscados.length===0))},dependencies:[it,nn,lt,rt,Vt,Lt,vt,jt,Pi,zt,Ht,Ln,Pn,et,_t,dt,wo,hn,Ut,Bt,Pt,mt,Ft,xt],encapsulation:2})};function PF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function LF(t,n){if(t&1&&(p(0,"mat-option",15),v(1),h()),t&2){let e=n.$implicit;E("value",e),_(),be(" ",e," ")}}function VF(t,n){if(t&1&&(p(0,"div",16),v(1),h()),t&2){let e=D(2);_(),be(" ",e.mensagens," ")}}function jF(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=D();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=D();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),v(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.pet.nome,r)||(o.pet.nome=r),de(r)}),h(),Oe(),p(5,"mat-icon",8),v(6,"pets"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),v(9,"Esp\xE9cie"),h(),p(10,"mat-select",9),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.pet.especie,r)||(o.pet.especie=r),de(r)}),H(11,LF,2,2,"mat-option",10),h(),Oe(),p(12,"mat-icon",8),v(13,"category"),h()(),H(14,VF,2,1,"div",11),p(15,"div",12)(16,"button",13)(17,"mat-icon"),v(18,"save"),h(),v(19," Salvar "),h(),p(20,"button",14)(21,"mat-icon"),v(22,"arrow_back"),h(),v(23," Voltar "),h()()()}if(t&2){let e=D();_(4),Re("ngModel",e.pet.nome),Fe(),_(6),Re("ngModel",e.pet.especie),Fe(),_(),E("ngForOf",e.especies),_(3),E("ngIf",e.mensagens)}}var Wc=class t{constructor(n,e,i,r){this.petService=n;this.route=e;this.router=i;this.cdr=r}petService;route;router;cdr;especies=["CACHORRO","GATO","PEIXE","ROEDOR","AVE","OUTRA"];pet={nome:"",especie:""};modoEdicao=!1;carregando=!1;mensagens="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&(this.modoEdicao=!0,this.carregando=!0,this.petService.porId(Number(n)).subscribe({next:e=>{this.pet=e,this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar pet",e),this.mensagens="Pet nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}))}onSubmit(){(this.modoEdicao?this.petService.atualizar(this.pet):this.petService.criar(this.pet)).subscribe({next:()=>{this.router.navigate(["/pets"])},error:e=>{console.error("Erro ao salvar pet",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w(Hs),w(Zt),w(Ve),w(me))};static \u0275cmp=R({type:t,selectors:[["app-pet-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["id","especie","name","especie","required","",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","error",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/pets","type","button"],[3,"value"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),v(3),h(),H(4,PF,2,0,"div",1)(5,jF,24,4,"form",2),h()()),e&2&&(_(3),be(" ",i.modoEdicao?"Editar Pet":"Novo Pet"," "),_(),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando))},dependencies:[it,nn,lt,Pt,ui,mt,Ft,di,Rn,xt,Kt,rt,Vt,Lt,et,_t,dt,hn,Ut,Bt,Us,If,Er,vt,jt,zt,Ht,Ln,Pn],styles:[".actions[_ngcontent-%COMP%]{display:flex;gap:12px;margin-top:20px}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:140px}"]})};var zs=()=>{let t=u(Ve);return u(ot).get("token")?!0:(t.navigate(["/login"]),!1)};var Af=()=>{let t=u(Ve),n=u(ot),e=n.get("token"),i=n.get("role");return e&&i==="ADMIN"?!0:(t.navigate(["/pets"]),!1)};var Ev=()=>(u(ot).clear(),!0);var $s=class t{constructor(n){this.http=n}http;todas(){return this.http.get(`${Ge.apiUrl}/empresa/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/empresa/${n}`)}porNome(n){return this.http.get(`${Ge.apiUrl}/empresa`,{params:{nome:n}})}criar(n){return this.http.post(`${Ge.apiUrl}/empresa`,n)}atualizar(n){return this.http.put(`${Ge.apiUrl}/empresa`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/empresa/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Tn))};static \u0275prov=P({token:t,factory:t.\u0275fac,providedIn:"root"})};var BF=t=>["/empresas",t];function UF(t,n){if(t&1){let e=Ue();p(0,"button",12),F("click",function(){le(e);let r=D();return r.termoBusca="",de(r.filtrarEmpresas())}),p(1,"mat-icon"),v(2,"close"),h()()}}function HF(t,n){if(t&1&&(p(0,"div",13),v(1),h()),t&2){let e=D();_(),be(" ",e.mensagens," ")}}function zF(t,n){t&1&&(p(0,"div",14),pe(1,"mat-spinner",15),h())}function $F(t,n){t&1&&(p(0,"div",26),v(1," Usu\xE1rios relacionados "),h())}function GF(t,n){if(t&1&&(p(0,"button",27)(1,"mat-icon"),v(2,"email"),h(),p(3,"span"),v(4),h()()),t&2){let e=n.$implicit;_(4),Xe(e.email)}}function WF(t,n){t&1&&(p(0,"div",28),v(1," Nenhum usu\xE1rio vinculado "),h())}function qF(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),v(2),h(),p(3,"td",19)(4,"button",20)(5,"mat-icon"),v(6,"group"),h()(),p(7,"mat-menu",null,0),H(9,$F,2,0,"div",21)(10,GF,5,1,"button",22)(11,WF,2,0,"div",23),h(),p(12,"button",24)(13,"mat-icon"),v(14,"edit"),h()(),p(15,"button",25),F("click",function(){let r=le(e).$implicit,o=D(2);return de(o.remover(r.id))}),p(16,"mat-icon"),v(17,"delete"),h()()()()}if(t&2){let e=n.$implicit,i=Sn(8);_(2),Xe(e.nome),_(2),E("matMenuTriggerFor",i),_(5),E("ngIf",e.usuarios?.length),_(),E("ngForOf",e.usuarios),_(),E("ngIf",!e.usuarios?.length),_(),E("routerLink",ao(6,BF,e.id))}}function YF(t,n){if(t&1&&(p(0,"table",16)(1,"thead")(2,"tr")(3,"th"),v(4,"Nome"),h(),p(5,"th",17),v(6,"A\xE7\xF5es"),h()()(),p(7,"tbody"),H(8,qF,18,8,"tr",18),h()()),t&2){let e=D();_(8),E("ngForOf",e.empresasFiltradas)}}function ZF(t,n){t&1&&(p(0,"p"),v(1,"Clique em "),p(2,"strong"),v(3,"Nova Empresa"),h(),v(4," para come\xE7ar."),h())}function KF(t,n){t&1&&(p(0,"p"),v(1,"Tente outro termo de busca."),h())}function QF(t,n){if(t&1&&(p(0,"div",29)(1,"mat-icon",30),v(2," group "),h(),p(3,"h3"),v(4),h(),H(5,ZF,5,0,"p",31)(6,KF,2,0,"p",31),h()),t&2){let e=D();_(4),Xe((e.termoBusca,"Nenhuma empresa encontrada")),_(),E("ngIf",!e.termoBusca),_(),E("ngIf",e.termoBusca)}}var Rf=class t{constructor(n,e,i){this.empresaService=n;this.localStorageService=e;this.cdr=i}empresaService;localStorageService;cdr;empresas=[];carregando=!0;mensagens="";empresasFiltradas=[];termoBusca="";get isAdmin(){return this.localStorageService.get("role")==="ADMIN"}ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.empresaService.todas().subscribe({next:n=>{console.log(n),this.empresas=n,this.filtrarEmpresas(),console.log(this.empresasFiltradas),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar empresas.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarEmpresas(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.empresasFiltradas=this.empresas;return}this.empresasFiltradas=this.empresas.filter(e=>e.nome?.toLowerCase().includes(n))}remover(n){n!=null&&confirm("Confirma\xE7ao para remover esta empresa")&&this.empresaService.remover(n).subscribe({next:()=>{this.empresas=this.empresas.filter(e=>e.id!==n),this.filtrarEmpresas(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover empresa",e),this.mensagens="Nao foi possivel remover esta empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w($s),w(ot),w(me))};static \u0275cmp=R({type:t,selectors:[["app-gestao-empresas"]],decls:20,vars:6,consts:[["menuUsuariosRelacionados","matMenu"],[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/empresas/nova"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome da empresa...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","accent","matTooltip","Usu\xE1rios relacionados",3,"matMenuTriggerFor"],["class","menu-header",4,"ngIf"],["mat-menu-item","",4,"ngFor","ngForOf"],["class","menu-empty",4,"ngIf"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"menu-header"],["mat-menu-item",""],[1,"menu-empty"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",1)(1,"mat-card")(2,"div",2)(3,"h2"),v(4,"Gest\xE3o de Empresas"),h(),p(5,"button",3)(6,"mat-icon"),v(7,"add"),h(),v(8," Nova Empresa "),h()(),p(9,"mat-form-field",4)(10,"mat-label"),v(11,"Pesquisar"),h(),p(12,"input",5),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.filtrarEmpresas()}),h(),Oe(),p(13,"mat-icon",6),v(14,"search"),h(),H(15,UF,3,0,"button",7),h(),H(16,HF,2,1,"div",8)(17,zF,2,0,"div",9)(18,YF,9,1,"table",10)(19,QF,7,3,"div",11),h()()),e&2&&(_(12),Re("ngModel",i.termoBusca),Fe(),_(3),E("ngIf",i.termoBusca),_(),E("ngIf",i.mensagens),_(),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando&&i.empresasFiltradas.length>0),_(),E("ngIf",!i.carregando&&i.empresasFiltradas.length===0))},dependencies:[Pt,mt,Ft,xt,rt,it,nn,lt,Vt,Lt,vt,jt,Pi,zt,Ht,Ln,Pn,et,_t,dt,wo,hn,Ut,Bt,xf,br,Mo,Df,Ls],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};function XF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function JF(t,n){if(t&1&&(p(0,"div",20),v(1),h()),t&2){let e=D(2);_(),be(" ",e.erroUsuario," ")}}function eP(t,n){if(t&1){let e=Ue();p(0,"mat-chip",21),F("removed",function(){let r=le(e).$implicit,o=D(2);return de(o.removerUsuario(r.id))}),v(1),p(2,"button",22)(3,"mat-icon"),v(4,"cancel"),h()()()}if(t&2){let e=n.$implicit;_(),be(" ",e.email," ")}}function tP(t,n){t&1&&(p(0,"p",23),v(1," Nenhum usu\xE1rio vinculado ainda. "),h())}function nP(t,n){if(t&1&&(p(0,"div",20),v(1),h()),t&2){let e=D(2);_(),be(" ",e.mensagens," ")}}function iP(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=D();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=D();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),v(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.empresa.nome,r)||(o.empresa.nome=r),de(r)}),h(),Oe(),h(),p(5,"div",8)(6,"h3"),v(7,"Usu\xE1rios relacionados"),h(),p(8,"div",9)(9,"mat-form-field",10)(10,"mat-label"),v(11,"E-mail do usu\xE1rio"),h(),p(12,"input",11),Ne("ngModelChange",function(r){le(e);let o=D();return Pe(o.novoEmail,r)||(o.novoEmail=r),de(r)}),F("keydown.enter",function(r){le(e);let o=D();return r.preventDefault(),de(o.adicionarUsuario())}),h(),Oe(),h(),p(13,"button",12),F("click",function(){le(e);let r=D();return de(r.adicionarUsuario())}),p(14,"mat-icon"),v(15,"add_circle"),h()()(),H(16,JF,2,1,"div",13),p(17,"mat-chip-set",14),H(18,eP,5,1,"mat-chip",15),h(),H(19,tP,2,0,"p",16),h(),H(20,nP,2,1,"div",13),p(21,"div",17)(22,"button",18)(23,"mat-icon"),v(24,"save"),h(),v(25," Salvar "),h(),p(26,"button",19)(27,"mat-icon"),v(28,"arrow_back"),h(),v(29," Voltar "),h()()()}if(t&2){let e=D();_(4),Re("ngModel",e.empresa.nome),Fe(),_(8),Re("ngModel",e.novoEmail),Fe(),_(4),E("ngIf",e.erroUsuario),_(2),E("ngForOf",e.empresa.usuarios),_(),E("ngIf",!e.empresa.usuarios?.length),_(),E("ngIf",e.mensagens)}}var qc=class t{constructor(n,e,i,r,o){this.empresaService=n;this.usuarioService=e;this.route=i;this.router=r;this.cdr=o}empresaService;usuarioService;route;router;cdr;empresa={nome:"",usuarios:[]};modoEdicao=!1;carregando=!1;mensagens="";novoEmail="";erroUsuario="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&!isNaN(Number(n))&&(this.modoEdicao=!0,this.carregando=!0,this.empresaService.porId(Number(n)).subscribe({next:e=>{this.empresa=e,this.empresa.usuarios||(this.empresa.usuarios=[]),this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar empresa",e),this.mensagens="Empresa nao encontrada.",this.carregando=!1,this.cdr.detectChanges()}}))}adicionarUsuario(){let n=this.novoEmail.trim();if(!n)return;if(this.empresa.usuarios?.some(i=>i.email.toLowerCase()===n.toLowerCase())){this.erroUsuario="Usu\xE1rio j\xE1 adicionado.";return}this.usuarioService.porEmail(n).subscribe({next:i=>{this.empresa.usuarios||(this.empresa.usuarios=[]),this.empresa.usuarios.push(i),this.novoEmail="",this.erroUsuario="",this.cdr.detectChanges()},error:i=>{console.error("Erro ao buscar usu\xE1rio",i),this.erroUsuario="Usu\xE1rio n\xE3o encontrado.",this.cdr.detectChanges()}})}removerUsuario(n){this.empresa.usuarios=this.empresa.usuarios?.filter(e=>e.id!==n)}onSubmit(){(this.modoEdicao?this.empresaService.atualizar(this.empresa):this.empresaService.criar(this.empresa)).subscribe({next:()=>{this.router.navigate(["/empresas"])},error:e=>{console.error("Erro ao salvar empresa",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar a empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(w($s),w(Cr),w(Zt),w(Ve),w(me))};static \u0275cmp=R({type:t,selectors:[["app-empresa-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],[1,"usuarios-section"],[1,"add-usuario"],["appearance","outline",1,"email-field"],["matInput","","name","novoEmail","placeholder","usuario@email.com",3,"ngModelChange","keydown.enter","ngModel"],["mat-icon-button","","color","primary","type","button",3,"click"],["class","error",4,"ngIf"],["aria-label","Usu\xE1rios relacionados"],[3,"removed",4,"ngFor","ngForOf"],["class","empty-usuarios",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/empresas","type","button"],[1,"error"],[3,"removed"],["matChipRemove",""],[1,"empty-usuarios"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),v(3),h(),H(4,XF,2,0,"div",1)(5,iP,30,6,"form",2),h()()),e&2&&(_(3),be(" ",i.modoEdicao?"Editar Empresa":"Nova Empresa"," "),_(),E("ngIf",i.carregando),_(),E("ngIf",!i.carregando))},dependencies:[it,nn,lt,Pt,ui,mt,Ft,di,Rn,xt,Kt,rt,Vt,Lt,et,_t,dt,Ut,Bt,Us,vt,jt,Pi,zt,Ht,Ln,Pn,Ls,Lc,nD,ff],styles:[".usuarios-section[_ngcontent-%COMP%]{margin:16px 0}.add-usuario[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px}.email-field[_ngcontent-%COMP%]{flex:1}.empty-usuarios[_ngcontent-%COMP%]{color:#0006;font-style:italic;font-size:14px}"]})};var ND=[{path:"",redirectTo:"login",pathMatch:"full"},{path:"login",component:df,canActivate:[Ev]},{path:"criar-usuario",component:uf,canActivate:[Ev]},{path:"pets",component:Tf,canActivate:[zs]},{path:"pets/novo",component:Wc,canActivate:[zs]},{path:"pets/:id",component:Wc,canActivate:[zs]},{path:"usuarios",component:Sf,canActivate:[zs]},{path:"usuarios/:id",component:Mf,canActivate:[zs]},{path:"empresas",component:Rf,canActivate:[Af]},{path:"empresas/novo",component:qc,canActivate:[Af]},{path:"empresas/:id",component:qc,canActivate:[Af]}];var kD=(t,n)=>{let i=u(ot).get("token");return i&&(t=t.clone({setHeaders:{Authorization:`Bearer ${i}`}})),n(t)};var OD={providers:[tp(),Og(ND),ag(cg([kD]))]};function rP(t,n){t&1&&(p(0,"a",11)(1,"mat-icon"),v(2,"work"),h(),v(3," Empresas "),h())}function oP(t,n){if(t&1&&(p(0,"span",12)(1,"mat-icon"),v(2,"account_circle"),h(),v(3),h()),t&2){let e=D(2);_(3),be(" ",e.emailUsuario," ")}}function sP(t,n){if(t&1){let e=Ue();p(0,"nav",1)(1,"div",2),pe(2,"span",3),h(),p(3,"div",4)(4,"a",5)(5,"mat-icon"),v(6,"pets"),h(),v(7," Pets "),h(),p(8,"a",6)(9,"mat-icon"),v(10,"group"),h(),v(11," Usu\xE1rios "),h(),H(12,rP,4,0,"a",7),h(),p(13,"div",8),H(14,oP,4,1,"span",9),p(15,"button",10),F("click",function(){le(e);let r=D();return de(r.sair())}),p(16,"mat-icon"),v(17,"logout"),h(),v(18," Sair "),h()()()}if(t&2){let e=D();_(12),E("ngIf",e.isAdmin),_(2),E("ngIf",e.logado)}}var Nf=class t{constructor(n,e,i){this.authService=n;this.router=e;this.localStorageService=i}authService;router;localStorageService;get logado(){return this.authService.isLoggedIn()}get emailUsuario(){return this.localStorageService.get("email")}sair(){this.authService.logout(),this.router.navigate(["/login"])}get isAdmin(){return this.localStorageService.get("role")==="ADMIN"}static \u0275fac=function(e){return new(e||t)(w(Fn),w(Ve),w(ot))};static \u0275cmp=R({type:t,selectors:[["app-root"]],decls:2,vars:1,consts:[["class","top-nav",4,"ngIf"],[1,"top-nav"],[1,"nav-left"],[1,"logo"],[1,"nav-center"],["routerLink","/pets","routerLinkActive","active"],["routerLink","/usuarios","routerLinkActive","active"],["routerLink","/empresas","routerLinkActive","active",4,"ngIf"],[1,"nav-right"],["class","nav-user",4,"ngIf"],["mat-raised-button","","color","primary",3,"click"],["routerLink","/empresas","routerLinkActive","active"],[1,"nav-user"]],template:function(e,i){e&1&&(H(0,sP,19,2,"nav",0),pe(1,"router-outlet")),e&2&&E("ngIf",i.logado)},dependencies:[fc,it,lt,rt,zt,Ht,vt,jt],encapsulation:2})};Xm(Nf,OD).catch(t=>console.error(t));
